"""
AdvancedAV FFmpeg commandline generator v2.0 [YoutubeDL PostProcessor Edition]
-----------------------------------------------------------
    AdvancedAV helps with constructing FFmpeg commandline arguments.

    It can automatically parse input files with the help of FFmpeg's ffprobe tool (limited..)
    and allows programatically mapping streams to output files and setting metadata on them.

    This is especially useful when multiple operations should be performed in one go,
    as is the case with youtube-dl PostProcessors. Chaining the classic FFmpeg*PPs
    one after another has the huge overhead of opening, copying and remuxing the input file
    over and over again.

    To use it outside youtube-dl, just copy the non-youtube-dl specific part
    and implement an object with AdvancedAVPP's to_screen, to_debug, call_probe and call_conv methods
    to pass as the AdvancedAVPP argument to Task and/or SimpleTask
-----------------------------------------------------------
    Copyright 2014-2015 Taeyeon Mori

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

Exception:
    The Youtube-DL glue code is released to the public domain (as it was adapted from existing code ;)
"""

import os
import re
import subprocess
import collections
import itertools
import json

from collections.abc import Iterable, Mapping, Sequence, Iterator, MutableMapping

# Constants
DEFAULT_CONTAINER = "matroska"

S_AUDIO = "a"
S_VIDEO = "v"
S_SUBTITLE = "s"
S_UNKNOWN = "u"


def stream_type(type_: str) -> str:
    """ Convert the ff-/avprobe type output to the notation used on the ffmpeg/avconv commandline """
    lookup = {
        "Audio": S_AUDIO,
        "Video": S_VIDEO,
        "Subtitle": S_SUBTITLE
    }

    return lookup.get(type_, S_UNKNOWN)


class AdvancedAVError(Exception):
    pass


class Stream:
    """
    Abstract stream base class

    One continuous stream of data muxed into a container format
    """
    __slots__ = "file", "type", "index", "pertype_index", "codec", "profile"

    def __init__(self, file: "File", type_: str, index: int=None, pertype_index: int=None,
                 codec: str=None, profile: str=None):
        self.file = file
        self.type = type_
        self.index = index
        self.pertype_index = pertype_index
        self.codec = codec
        self.profile = profile

    def update_indices(self, index: int, pertype_index: int=None):
        """ Update the Stream indices """
        self.index = index
        if pertype_index is not None:
            self.pertype_index = pertype_index

    @property
    def stream_spec(self):
        """ The StreamSpecification in the form of "<type>:<#stream_of_type>" or "<#stream>" """
        if self.pertype_index is not None:
            return "{}:{}".format(self.type, self.pertype_index)
        else:
            return str(self.index)

    def __str__(self):
        return "<%s %s#%i: %s %s (%s)>" % (type(self).__name__, self.file.name, self.index,
                                           self.type, self.codec, self.profile)


class InputStream(Stream):
    """
    Holds information about an input stream
    """
    __slots__ = "language"

    def __init__(self, file: "InputFile", type_: str, index: int, language: str, codec: str, profile: str):
        super().__init__(file, type_, index, codec=codec, profile=profile)
        self.file = file
        self.language = language

    def update_indices(self, index: int, pertype_index: int=None):
        """ InputStreams should not have their indices changed. """
        if index != self.index:
            raise ValueError("Cannot update indices on InputStreams! (This might mean there are bogus ids in the input")
        # pertype_index gets updated by File._add_stream() so we don't throw up if it gets updated


class OutputStream(Stream):
    """
    Holds information about a mapped output stream
    """
    __slots__ = "source", "metadata", "options"

    # TODO: support other parameters like frame resolution

    # Override polymorphic types
    #file = None
    """ :type: OutputFile """

    def __init__(self, file: "OutputFile", source: InputStream, stream_id: int, stream_pertype_id: int=None,
                 codec: str=None, options: Mapping=None, metadata: MutableMapping=None):
        super().__init__(file, source.type, stream_id, stream_pertype_id, codec)
        self.source = source
        self.options = options if options is not None else {}
        self.metadata = metadata if metadata is not None else {}

    # -- Manage Stream Metadata
    def set_meta(self, key: str, value: str):
        """ Set a metadata key """
        self.metadata[key] = value

    def get_meta(self, key: str) -> str:
        """ Retrieve a metadata key """
        return self.metadata[key]


class File:
    """
    ABC for Input- and Output-Files
    """
    __slots__ = "name", "options", "_streams", "_streams_by_type"

    def __init__(self, name: str, options: dict=None):
        self.name = name

        self.options = options if options is not None else {}
        """ :type: dict[str, str] """

        self._streams = []
        """ :type: list[Stream] """

        self._streams_by_type = collections.defaultdict(list)
        """ :type: dict[str, list[Stream]] """

    def _add_stream(self, stream: Stream):
        """ Add a stream """
        stream.update_indices(len(self._streams), len(self._streams_by_type[stream.type]))
        self._streams.append(stream)
        self._streams_by_type[stream.type].append(stream)

    @property
    def streams(self) -> Sequence:
        """ The streams contained in this file

        :rtype: Sequence[Stream]
        """
        return self._streams

    @property
    def video_streams(self) -> Sequence:
        """ All video streams

        :rtype: Sequence[Stream]
        """
        return self._streams_by_type[S_VIDEO]

    @property
    def audio_streams(self) -> Sequence:
        """ All audio streams

        :rtype: Sequence[Stream]
        """
        return self._streams_by_type[S_AUDIO]

    @property
    def subtitle_streams(self) -> Sequence:
        """ All subtitle streams

        :rtype: Sequence[Stream]
        """
        return self._streams_by_type[S_SUBTITLE]

    @property
    def filename(self) -> str:
        """ Alias for .name """
        return self.name

    def __str__(self):
        return "<%s %s>" % (type(self).__name__, self.name)


class InputFile(File):
    """
    Holds information about an input file
    """
    __slots__ = "pp", "_streams_initialized"

    def __init__(self, pp: "AdvancedAVPP", filename: str, options: Mapping=None):
        super().__init__(filename, dict(options.items()) if options else None)

        self.pp = pp

        self._streams_initialized = False

    # -- Probe streams
    _reg_probe_streams = re.compile(
        r"Stream #0:(?P<id>\d+)(?:\((?P<lang>\w+)\))?: (?P<type>\w+): (?P<codec>\w+)"
        r"(?: \((?P<profile>[^\)]+)\))?(?P<extra>.+)?"
    )

    def _initialize_streams_1(self, probe: str=None):
        """ Parse the ffprobe output

        The locale of the probe output in \param probe should be C!
        """
        if probe is None:
            probe = self.pp.call_probe(("-i", self.name))

        for match in self._reg_probe_streams.finditer(probe):
            self._add_stream(InputStream(self,
                                         stream_type(match.group("type")),
                                         int(match.group("id")),
                                         match.group("lang"),
                                         match.group("codec"),
                                         match.group("profile")))
        self._streams_initialized = True

    _probe_json = "-print_format", "json", "-show_streams"

    def _initialize_streams(self, probe: str=None) -> Iterator:
        # Ad-Hoc fix to use JSON output instead of regexps
        if probe is None:
            probe = self.pp.call_probe(("-i", self.name) + self._probe_json, ret=1)

        for stream in json.loads(probe)["streams"]:
            self._add_stream(InputStream(self,
                                         stream["codec_type"][0],
                                         stream["index"],
                                         stream["tags"]["language"] if ("tags" in stream and "language" in stream["tags"]) else None,
                                         stream["codec_name"],
                                         stream["profile"] if "profile" in stream else None))

        self._streams_initialized = True

    @property
    def streams(self) -> Sequence:
        """ Collect the available streams

        :rtype: Sequence[InputStream]
        """
        if not self._streams_initialized:
            self._initialize_streams()
        return self._streams

    @property
    def video_streams(self) -> Sequence:
        """ All video streams

        :rtype: Sequence[InputStream]
        """
        if not self._streams_initialized:
            self._initialize_streams()
        return self._streams_by_type[S_VIDEO]

    @property
    def audio_streams(self) -> Sequence:
        """ All audio streams

        :rtype: Sequence[InputStream]
        """
        if not self._streams_initialized:
            self._initialize_streams()
        return self._streams_by_type[S_AUDIO]

    @property
    def subtitle_streams(self) -> Sequence:
        """ All subtitle streams

        :rtype: Sequence[InputStream]
        """
        if not self._streams_initialized:
            self._initialize_streams()
        return self._streams_by_type[S_SUBTITLE]


class OutputFile(File):
    """
    Holds information about an output file
    """
    __slots__ = "task", "container", "metadata", "_mapped_sources"

    def __init__(self, task: "Task", name: str, container=DEFAULT_CONTAINER, options: Mapping=None):
        # Set default options
        _options = {"c": "copy"}

        if options is not None:
            _options.update(options)

        # Contstuct
        super().__init__(name, _options)

        self.task = task

        self.container = container
        self.metadata = {}
        """ :type: dict[str, str] """

        self._mapped_sources = set()
        """ :type: set[InputStream] """

    # -- Map Streams
    def map_stream_(self, stream: InputStream, codec: str=None, options: Mapping=None) -> OutputStream:
        """ map_stream() minus add_input_file

        map_stream() needs to ensure that the file the stream originates from is registered as input to this Task.
        However, when called repeatedly on streams of the same file, that is superflous.
        """
        out = OutputStream(self, stream, -1, -1, codec, options)

        self._add_stream(out)
        self._mapped_sources.add(stream)

        self.task.pp.to_debug("Mapping Stream %s => %s (%i)",
                              self.task.qualified_input_stream_spec(stream),
                              out.stream_spec,
                              self.task.outputs.index(self))
        return out

    def map_stream(self, stream: InputStream, codec: str=None, options: Mapping=None) -> OutputStream:
        """ Map an input stream to the output

        Note that this will add multiple copies of an input stream to the output when called multiple times
        on the same input stream. Check with is_stream_mapped() beforehand if the stream might already be mapped.
        """
        self.task.add_input(stream.file)
        return self.map_stream_(stream, codec, options)

    def is_stream_mapped(self, stream: InputStream) -> bool:
        """ Test if an input stream is already mapped """
        return stream in self._mapped_sources

    def get_mapped_stream(self, stream: InputStream) -> OutputStream:
        """ Get the output stream this input stream is mapped to """
        for out in self._streams:
            if out.source == stream:
                return out

    # -- Map multiple Streams
    def map_all_streams(self, file: "str | InputFile", return_existing: bool=False) -> Sequence:
        """ Map all streams in \param file

        Note that this will only map streams that are not already mapped.

        :rtype: Sequence[OutputStream]
        """
        out_streams = []
        for stream in self.task.add_input(file).streams:
            if stream in self._mapped_sources:
                if return_existing:
                    out_streams.append(self.get_mapped_stream(stream))
            else:
                out_streams.append(self.map_stream_(stream))

        return out_streams

    def merge_all_files(self, files: Iterable, return_existing: bool=False) -> Sequence:
        """ Map all streams from multiple files

        Like map_all_streams(), this will only map streams that are not already mapped.

        :type files: Iterable[str | InputFile]
        :rtype: Sequence[OutputStream]
        """
        out_streams = []
        for file in files:
            for stream in self.task.add_input(file).streams:
                if stream in self._mapped_sources:
                    if return_existing:
                        out_streams.append(self.get_mapped_stream(stream))
                else:
                    out_streams.append(self.map_stream_(stream))

        return out_streams

    # -- Sort Streams
    def reorder_streams(self):
        """ Sort the mapped streams by type """
        self._streams.clear()

        for stream in itertools.chain(self.video_streams,
                                      self.audio_streams,
                                      self.subtitle_streams):
            stream.update_indices(len(self._streams))
            self._streams.append(stream)

    # -- Manage Global Metadata
    def set_meta(self, key: str, value: str):
        self.metadata[key] = value

    def get_meta(self, key: str) -> str:
        return self.metadata[key]


class Task:
    """
    Holds information about an AV-processing Task.

    A Task is a collection of Input- and Output-Files and related options.
    While OutputFiles are bound to one task at a time, InputFiles can be reused across Tasks.
    """
    def __init__(self, pp: "AdvancedAVPP"):
        self.pp = pp

        self.inputs = []
        """ :type: list[InputFile] """
        self.inputs_by_name = {}
        """ :type: dict[str, InputFile] """

        self.outputs = []
        """ :type: list[OutputFile] """

    # -- Manage Inputs
    def add_input(self, file: "str | InputFile") -> InputFile:
        """ Register an input file

        When \param file is already registered as input file to this Task, do nothing.

        :param file: Can be either the filename of an input file or an InputFile object.
                        The latter will be created if the former is passed.
        """
        if not isinstance(file, InputFile):
            if file in self.inputs_by_name:
                return self.inputs_by_name[file]

            file = InputFile(self.pp, file)

        if file not in self.inputs:
            self.pp.to_debug("Adding input file #%i: %s", len(self.inputs), file.name)
            self.inputs.append(file)
            self.inputs_by_name[file.name] = file

        return file

    def qualified_input_stream_spec(self, stream: InputStream) -> str:
        """ Construct the qualified input stream spec (combination of input file number and stream spec)

        None will be returned if stream's file isn't registered as an input to this Task
        """
        file_index = self.inputs.index(stream.file)
        if file_index >= 0:
            return "{}:{}".format(file_index, stream.stream_spec)

    # -- Manage Outputs
    def add_output(self, filename: str, container: str=DEFAULT_CONTAINER, options: Mapping=None) -> OutputFile:
        """ Add an output file

        NOTE: Contrary to add_input this will NOT take an OutputFile instance and return it.
        """
        for outfile in self.outputs:
            if outfile.name == filename:
                raise AdvancedAVError("Output File '%s' already added." % filename)
        else:
            outfile = OutputFile(self, filename, container, options)
            self.pp.to_debug("New output file #%i: %s", len(self.outputs), filename)
            self.outputs.append(outfile)
            return outfile

    # -- Manage Streams
    def iter_video_streams(self) -> Iterator:
        for input_ in self.inputs:
            yield from input_.video_streams

    def iter_audio_streams(self) -> Iterator:
        for input_ in self.inputs:
            yield from input_.audio_streams

    def iter_subtitle_streams(self) -> Iterator:
        for input_ in self.inputs:
            yield from input_.subtitle_streams

    # -- FFmpeg
    @staticmethod
    def argv_options(options: Mapping, qualifier: str=None) -> Iterator:
        """ Yield arbitrary options

        :type options: Mapping[str, str]
        :rtype: Iterator[str]
        """
        if qualifier is None:
            opt_fmt = "-%s"
        else:
            opt_fmt = "-%%s:%s" % qualifier
        for option, value in options.items():
            yield opt_fmt % option
            if value is not None:
                yield value

    @staticmethod
    def argv_metadata(metadata: Mapping, qualifier: str=None) -> Iterator:
        """ Yield arbitrary metadata

        :type metadata: Mapping[str, str]
        :rtype: Iterator[str]
        """
        if qualifier is None:
            opt = "-metadata"
        else:
            opt = "-metadata:%s" % qualifier
        for meta in metadata.items():
            yield opt
            yield "%s=%s" % meta

    def generate_args(self) -> Iterator:
        """ Generate the ffmpeg commandline for this task

        :rtype: Iterator[str]
        """
        # Inputs
        for input_ in self.inputs:
            # Input options
            yield from self.argv_options(input_.options)

            # Add Input
            yield "-i"
            filename = encodeFilename(input_.name, True)
            if filename[0] == '-':
                yield "./" + filename
            else:
                yield filename

        # Outputs
        for output in self.outputs:
            # Map Streams, sorted by type
            output.reorder_streams()

            for stream in output.streams:
                yield "-map"
                yield self.qualified_input_stream_spec(stream.source)

                if stream.codec is not None:
                    yield "-c:%s" % stream.stream_spec
                    yield stream.codec

                yield from self.argv_metadata(stream.metadata, stream.stream_spec)
                yield from self.argv_options(stream.options, stream.stream_spec)

            # Global Metadata & Additional Options
            yield from self.argv_metadata(output.metadata)
            yield from self.argv_options(output.options)

            # Container
            if output.container is not None:
                yield "-f"
                yield output.container

            # Output Filename, prevent it from being interpreted as option
            out_fn = encodeFilename(output.name)
            yield out_fn if out_fn[0] != "-" else "./" + out_fn

    def commit(self, additional_args: Iterable=()):
        """
        Commit the changes.

        additional_args is used to pass global arguments to ffmpeg. (like -y)

        :type additional_args: Iterable[str]
        :raises: AdvancedAVError when FFmpeg fails
        """
        self.pp.call_conv(itertools.chain(additional_args, self.generate_args()))


class SimpleTask(Task):
    """
    A simple task with only one output file

    All members of the OutputFile can be accessed on the SimpleTask directly, as well as the usual Task methods.
    Usage of add_output should be avoided however, because it would lead to confusion.
    """
    def __init__(self, pp: "AdvancedAVPP", filename: str, container: str=DEFAULT_CONTAINER, options: Mapping=None):
        super().__init__(pp)

        self.output = self.add_output(filename, container, options)

    def __getattr__(self, attr: str):
        """ You can directly access the OutputFile from the SimpleTask instance """
        return getattr(self.output, attr)


# ---- Youtube-DL Glue from here ----
from ..version import __version__
from .common import PostProcessor

from .ffmpeg import EXT_TO_OUT_FORMATS, ACODECS

from ..utils import (
    check_executable,
    encodeFilename,
    prepend_extension,
    shell_quote,
    subtitles_filename,
    PostProcessingError,
)


def detect_all_progs(progs: Iterable, args: Sequence) -> list:
    """ Find a number of programs and return only if all were found.

    :param progs: Iterable[str] List of program names to look for
    :param args: Sequence[str] List of arguments to pass to the programs (--version or similar)
    :return: list[str] List of full program paths for progs or None
    """
    result = [check_executable(prog, args) for prog in progs]
    return all(result) and result or None


class AdvancedAVPP(PostProcessor):
    """
    Advanced PostProcessor uniting the tasks of all FFmpeg*PPs

    This improves speed over chaining several FFmpeg PPs.

    It could also easily be extended further (transcoding etc)

    Options [bool unless noted otherwise]:
        verbose: print additional output
        prefer_ffmpeg: Prefer FFMpeg over libAV
        keep_originals: Don't delete downloaded files after postprocessing

        add_metadata: Add metadata to the resulting container file
        repack_container: [str] Repack into a different type of container (pass the new file extension)
        embed_subs: Embed any downloaded subtitles into the container
        extract_audio: Extract the first audio stream to a separate file

        audio_preferred_format: [str] "best" or a format name for the audio file
        audio_preferred_quality: [str containing int] 0-10: FFmpeg quality; 10+: bitrate in kbps
    """
    def __init__(self, downloader, options: Mapping=None):
        super(AdvancedAVPP, self).__init__(downloader)
        self._opts = options
        self._prog_conv = None
        self._prog_probe = None

    # ---- Logging ----
    def to_screen(self, text: str, *fmt):
        """ Output formatted text. """
        self._downloader.to_screen("[AdvAV] " + text % fmt)

    def to_debug(self, text: str, *fmt):
        """ Debug-output formatted text. """
        if self._opts.get("verbose"):
            self._downloader.to_screen("[AdvAV] (debug) " + text % fmt)

    # ---- Programs ----
    _av_progs = ["avconv", "avprobe"]
    _ff_progs = ["ffmpeg", "ffprobe"]
    _detect_progs_args = ["-version"]

    def _detect_progs(self):
        """
        Detect the programs needed by the AdvancedAV preprocessor (ffmpeg or libav)
        """

        if self._opts.get("prefer_ffmpeg"):
            progs = detect_all_progs(self._ff_progs, self._detect_progs_args) \
                or detect_all_progs(self._av_progs, self._detect_progs_args)
        else:
            progs = detect_all_progs(self._av_progs, self._detect_progs_args) \
                or detect_all_progs(self._ff_progs, self._detect_progs_args)

        if not progs:
            raise AdvancedAVError("Could not find Libav or FFmpeg. AdvAV needs either one to work."
                                  "You can remove --use-aavpp to disable AdvAV")

        self._prog_conv, self._prog_probe = progs

    _posix_env = dict(os.environ)
    _posix_env["LANG"] = _posix_env["LC_ALL"] = "C"

    def _exec(self, argv: Sequence, env: Mapping=None, ret=2) -> str:
        """
        Execute a shell command
        :param argv: The program and arguments
        :param env: Optionally, a environment override
        :return: The stderr output of the program
        """
        self.to_debug("Running Command: %s", shell_quote(argv))

        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

        out, err = proc.communicate()

        if proc.returncode != 0:
            err = err.decode("utf-8", "replace")
            msg = err.strip().split('\n')[-1]
            raise AdvancedAVError(msg)

        if ret == 2:
            return err.decode("utf-8", "replace")
        else:
            return out.decode("utf-8", "replace")

    def call_conv(self, args: Iterable) -> str:
        """ Actually call avconv/ffmpeg

        :type args: Iterable[str]
        """
        if not self._prog_conv:
            self._detect_progs()

        return self._exec(tuple(itertools.chain((self._prog_conv,), args)))

    def call_probe(self, args: Iterable, ret=2) -> str:
        """ Call ffprobe/avprobe (With LANG=LC_ALL=C)

        :type args: Iterable[str]
        """
        if not self._prog_probe:
            self._detect_progs()

        return self._exec(tuple(itertools.chain((self._prog_probe,), args)), env=self._posix_env, ret=ret)

    # ---- Work ----
    # noinspection PyMethodMayBeStatic ## We might want to use _opts inside
    def add_metadata(self, out: OutputFile, info: Mapping):
        """ Add Metadata, see FFmpegMetadataPP """
        if info.get("title") is not None:
            out.set_meta("title", info["title"])

        if info.get("upload_date") is not None:
            out.set_meta("date", info["upload_date"])

        if info.get("uploader") is not None:
            out.set_meta("artist", info["uploader"])
        elif info.get("uploader_id") is not None:
            out.set_meta("author", info["uploader_id"])

        if info.get("webpage_url") is not None:
            out.set_meta("url", info["webpage_url"])

        if info.get("description") is not None:
            out.set_meta("description", info["description"])

        out.set_meta("encoded_by", "youtube-dl %s" % __version__)
        out.set_meta("copyright", "the original artist")

    def embed_subs_file(self, out: OutputFile, path: str, lang: str, src_codec: str):
        """ Add subtitle streams from a single file """
        # See if user specified codec
        codec = (self._opts.get("subs_codec", None) or src_codec).lower()

        # Check compatibility
        if out.container.lower() in (DEFAULT_CONTAINER, "webm"):
            if codec not in ("ass", "ssa", "srt", "vtt"):
                self.to_screen("Matroska (mkv, webm) container cannot handle %s subtitles. Converting to SSA"
                               % codec.upper())
                codec = "ssa"
            elif codec == src_codec:
                codec = "copy"
        elif out.container.lower() in ("mp4", "m4v", "mov"):
            if codec != "mov_text":
                self.to_screen("MP4 container support for subtitles is bad. Trying to add subtitles as MOV_TEXT")
                codec = "mov_text"
        else:
            raise AdvancedAVError("Embedding subtitles is not supported for %s containers!" % out.container.upper())

        lang_code = LangMap.convert_lang_code(lang)
        infile = out.task.add_input(subtitles_filename(path, lang, src_codec))

        for stream in infile.streams:
            out = out.map_stream_(stream, codec)
            if lang_code:
                out.set_meta("language", lang_code)

    def embed_subs(self, out: OutputFile, info: Mapping):
        """ Add the subtitle streams from the external files """
        # Add subtitle streams
        for lang, sinfo in (info['requested_subtitles'] or {}).items():
            self.embed_subs_file(out, info["filepath"], lang, sinfo["ext"])

    def extract_audio(self, task: Task, filename: str) -> OutputFile:
        # Determine format (XXX: Just take first audio stream?)
        try:
            audio_stream = next(task.iter_audio_streams())
        except StopIteration:
            self.to_screen("<Error> No audio stream found, cannot extract audio.")
            return None

        # see ffmpeg.py:FFmpegExtractAudioPP
        pref_fmt = self._opts.get("audio_preferred_format", "best")
        pref_q = self._opts.get("audio_preferred_quality", None)
        audio_codec = "copy"
        audio_container = None
        audio_options = {}

        if pref_fmt == "best" or pref_fmt == audio_stream.codec or \
                (pref_fmt == "m4a" and audio_stream.codec == "aac"):
            if audio_stream.codec == "aac" and pref_fmt in ("m4a", "best"):
                audio_ext = "m4a"
                audio_options["bsf"] = "aac_adtstoasc"
            elif pref_fmt in ("aac", "mp3", "vorbis", "ogg", "opus"):
                audio_ext = pref_fmt
                if pref_fmt == "vorbis":
                    audio_ext = "ogg"
                if pref_fmt == "aac":
                    audio_container = "adts"
            else:
                # MP3 otherwise?
                audio_codec = "libmp3lame"
                audio_ext = "mp3"

                if pref_q is not None:
                    if int(pref_q) < 10:
                        audio_options["q"] = pref_q
                    else:
                        audio_options["b"] = pref_q + "k"
        else:
            # Encode
            ACODECS.get(pref_fmt, pref_fmt)
            audio_container = {
                'aac': 'adts',
                'vorbis': 'ogg'
            }.get(pref_fmt, None)
            audio_ext = {
                'vorbis': 'ogg'
            }.get(pref_fmt, pref_fmt)

            if pref_q is not None:
                # The opus codec doesn't support the quality option
                if int(pref_q) < 10 and pref_fmt != 'opus':
                    audio_options["q"] = pref_q
                else:
                    audio_options["b"] = pref_q + "k"

            if pref_fmt == "m4a":
                audio_options["bsf"] = "aac_adtstoasc"

        audio_filename = filename[:filename.rindex(".")+1] + audio_ext
        audio = task.add_output(audio_filename, audio_container)

        audio.map_stream(audio_stream, audio_codec, audio_options)

        return audio

    def fixup_m3u8(self, task: Task):
        for out in task.outputs:
            for s in out.audio_streams:
                if s.codec == "aac":
                    s.options["bsf"] = "aac_adtstoasc"

    # ---- Main ----
    default_args = ["-y"]

    def run(self, information: MutableMapping):
        """ PostProcessor entrypoint """
        # --- Create a task ---
        task = Task(self)
        task_description = []

        # --- Main Output ---
        # Determine format. We need to do this first as we want to pass name and format to the Task constructor
        container = information["ext"]
        if self._opts["repack_container"]:
            container = information["ext"] = self._opts["repack_container"]

            # The Matroska format is actually called DEFAULT_CONTAINER, not "mkv", which is the file extension only.
            if container == "mkv":
                container = "matroska"

        # Get the output filename
        filename = self._downloader.prepare_filename(information)
        """ :type: str """

        main = task.add_output(filename + ".aavtemp", container)

        # Merging
        if "__files_to_merge" in information:
            main.merge_all_files(information["__files_to_merge"])
            del information["__files_to_merge"]
            task_description.append("merge")
        else:
            # Add main video file
            print(main.map_all_streams(information["filepath"]))
            print(task.inputs[0].streams)

        # Repacking
        if self._opts.get("repack_container"):
            # The real work was already done above in the "Determine format" section.
            task_description.append("repack")

        # Embed subtitles
        if self._opts.get("embed_subs") and "requested_subtitles" in information:
            self.embed_subs(main, information)
            task_description.append("embed subtitles")

        # Metadata
        if self._opts.get("add_metadata"):
            self.add_metadata(main, information)
            task_description.append("add metadata")

        # --- Audio Output ---
        if self._opts["extract_audio"]:
            audio = self.extract_audio(task, filename)

            if audio:
                if self._opts.get("add_metadata"):
                    self.add_metadata(audio, information)

                task_description.append("extract audio")

        # Fixup
        if "__fixups" in information:
            for fu in information["__fixups"]:
                if fu == "m3u8_aac":
                    # Must come after extract_audio
                    self.fixup_m3u8(task)
                    task_description.append("fix aac")
                else:
                    raise AdvancedAVError("Unknown Fixup: %s" % fu)

        # --- Run ---
        self.to_screen("Running AAV tasks for '%s' (%s)...", filename, ", ".join(task_description))
        task.commit(self.default_args)

        # Clean up files
        # If we need to keep the originals, rename any conflicting input files
        if self._opts.get("keep_originals"):
            if filename in task.inputs_by_name:
                os.rename(filename, prepend_extension(filename, "orig"))
        else:
            for input_ in task.inputs:
                os.remove(input_.name)

        # Rename the temporary file to the actual output filename
        os.rename(main.filename, filename)

        # Export information
        information["filepath"] = filename
        information["filename"] = filename.rsplit(u'/', 1)[-1]

        # TODO: should return list of files to delete instead of handling deleting itself; but what about conflicts?
        return [], information

    def _run(self, information: MutableMapping):
        """ Helper method to debug exeptions in run() """
        try:
            return self._run(information)
        except AdvancedAVError as e:
            raise PostProcessingError("[AdvAV] " + e.args[0])
        except:
            import traceback

            traceback.print_exc()
            raise


# ---- Language Map ----
# Taken from the FFmpeg subtitle postprocessor
# See http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
class LangMap:
    _lang_map = {
        'aa': 'aar',
        'ab': 'abk',
        'ae': 'ave',
        'af': 'afr',
        'ak': 'aka',
        'am': 'amh',
        'an': 'arg',
        'ar': 'ara',
        'as': 'asm',
        'av': 'ava',
        'ay': 'aym',
        'az': 'aze',
        'ba': 'bak',
        'be': 'bel',
        'bg': 'bul',
        'bh': 'bih',
        'bi': 'bis',
        'bm': 'bam',
        'bn': 'ben',
        'bo': 'bod',
        'br': 'bre',
        'bs': 'bos',
        'ca': 'cat',
        'ce': 'che',
        'ch': 'cha',
        'co': 'cos',
        'cr': 'cre',
        'cs': 'ces',
        'cu': 'chu',
        'cv': 'chv',
        'cy': 'cym',
        'da': 'dan',
        'de': 'deu',
        'dv': 'div',
        'dz': 'dzo',
        'ee': 'ewe',
        'el': 'ell',
        'en': 'eng',
        'eo': 'epo',
        'es': 'spa',
        'et': 'est',
        'eu': 'eus',
        'fa': 'fas',
        'ff': 'ful',
        'fi': 'fin',
        'fj': 'fij',
        'fo': 'fao',
        'fr': 'fra',
        'fy': 'fry',
        'ga': 'gle',
        'gd': 'gla',
        'gl': 'glg',
        'gn': 'grn',
        'gu': 'guj',
        'gv': 'glv',
        'ha': 'hau',
        'he': 'heb',
        'hi': 'hin',
        'ho': 'hmo',
        'hr': 'hrv',
        'ht': 'hat',
        'hu': 'hun',
        'hy': 'hye',
        'hz': 'her',
        'ia': 'ina',
        'id': 'ind',
        'ie': 'ile',
        'ig': 'ibo',
        'ii': 'iii',
        'ik': 'ipk',
        'io': 'ido',
        'is': 'isl',
        'it': 'ita',
        'iu': 'iku',
        'ja': 'jpn',
        'jv': 'jav',
        'ka': 'kat',
        'kg': 'kon',
        'ki': 'kik',
        'kj': 'kua',
        'kk': 'kaz',
        'kl': 'kal',
        'km': 'khm',
        'kn': 'kan',
        'ko': 'kor',
        'kr': 'kau',
        'ks': 'kas',
        'ku': 'kur',
        'kv': 'kom',
        'kw': 'cor',
        'ky': 'kir',
        'la': 'lat',
        'lb': 'ltz',
        'lg': 'lug',
        'li': 'lim',
        'ln': 'lin',
        'lo': 'lao',
        'lt': 'lit',
        'lu': 'lub',
        'lv': 'lav',
        'mg': 'mlg',
        'mh': 'mah',
        'mi': 'mri',
        'mk': 'mkd',
        'ml': 'mal',
        'mn': 'mon',
        'mr': 'mar',
        'ms': 'msa',
        'mt': 'mlt',
        'my': 'mya',
        'na': 'nau',
        'nb': 'nob',
        'nd': 'nde',
        'ne': 'nep',
        'ng': 'ndo',
        'nl': 'nld',
        'nn': 'nno',
        'no': 'nor',
        'nr': 'nbl',
        'nv': 'nav',
        'ny': 'nya',
        'oc': 'oci',
        'oj': 'oji',
        'om': 'orm',
        'or': 'ori',
        'os': 'oss',
        'pa': 'pan',
        'pi': 'pli',
        'pl': 'pol',
        'ps': 'pus',
        'pt': 'por',
        'qu': 'que',
        'rm': 'roh',
        'rn': 'run',
        'ro': 'ron',
        'ru': 'rus',
        'rw': 'kin',
        'sa': 'san',
        'sc': 'srd',
        'sd': 'snd',
        'se': 'sme',
        'sg': 'sag',
        'si': 'sin',
        'sk': 'slk',
        'sl': 'slv',
        'sm': 'smo',
        'sn': 'sna',
        'so': 'som',
        'sq': 'sqi',
        'sr': 'srp',
        'ss': 'ssw',
        'st': 'sot',
        'su': 'sun',
        'sv': 'swe',
        'sw': 'swa',
        'ta': 'tam',
        'te': 'tel',
        'tg': 'tgk',
        'th': 'tha',
        'ti': 'tir',
        'tk': 'tuk',
        'tl': 'tgl',
        'tn': 'tsn',
        'to': 'ton',
        'tr': 'tur',
        'ts': 'tso',
        'tt': 'tat',
        'tw': 'twi',
        'ty': 'tah',
        'ug': 'uig',
        'uk': 'ukr',
        'ur': 'urd',
        'uz': 'uzb',
        've': 'ven',
        'vi': 'vie',
        'vo': 'vol',
        'wa': 'wln',
        'wo': 'wol',
        'xh': 'xho',
        'yi': 'yid',
        'yo': 'yor',
        'za': 'zha',
        'zh': 'zho',
        'zu': 'zul',
    }

    @classmethod
    def convert_lang_code(cls, code):
        """Convert language code from ISO 639-1 to ISO 639-2/T"""
        return cls._lang_map.get(code[:2])
