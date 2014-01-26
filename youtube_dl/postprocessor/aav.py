# Written by Mori Taeyeon.
# The file is released to the public domain.
# However, I'd greatly appreciate it if you gave credit where credit is due.

import os
import re
import subprocess
import collections

from ..version import __version__
from .common import PostProcessor

from ..utils import (
    check_executable,
    encodeFilename,
    PostProcessingError,
    prepend_extension,
    shell_quote,
    subtitles_filename,
)

# Constants
S_AUDIO = "a"
S_VIDEO = "v"
S_SUBTITLE = "s"
S_UNKNOWN = "u"


class AdvancedAVError(PostProcessingError):
    pass


class AdvancedAVPP(PostProcessor):
    """
    Advanced PostProcessor uniting the tasks of all FFmpeg*PPs

    This improves speed over chaining several FFmpeg PPs.

    It could also easily be extended further (transcoding etc)
    """

    def __init__(self, downloader=None, options={}):
        super(AdvancedAVPP, self).__init__(downloader)
        self._opts = options
        self._prog_conv = None
        self._prog_probe = None

    # ---- Logging ----
    def to_screen(self, text, *fmt):
        self._downloader.to_screen("[AdvAV] " + text % fmt)

    def to_debug(self, text, *fmt):
        if self._downloader.params.get("verbose"):
            self._downloader.to_screen("[AdvAV] (debug) " + text % fmt)

    # ---- Programs ----
    @staticmethod
    def _detect_all_progs(progs, args):
        result = [check_executable(prog, args) for prog in progs]
        if [1 for prog in result if not prog]:
            return None
        return result

    def _detect_progs(self):
        av_progs = ["avconv", "avprobe"]
        ff_progs = ["ffmpeg", "ffprobe"]
        args     = ["-version"]

        if self._opts.get("prefer_ffmpeg"):
            progs = self._detect_all_progs(ff_progs, args) or self._detect_all_progs(av_progs, args)
        else:
            progs = self._detect_all_progs(av_progs, args) or self._detect_all_progs(ff_progs, args)
        
        if not progs:
            raise AdvancedAVError("Could not find Libav or FFmpeg. AdvAV needs either one to work. You can remove --use-aavpp to disable AdvAV")
        else:
            self._prog_conv, self._prog_probe = progs

    def _exec(self, argv, env=None):
        self.to_debug("Running Command: %s", shell_quote(argv))

        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

        out, err = proc.communicate()

        if proc.returncode != 0:
            err = err.decode("utf-8", "replace")
            msg = err.strip().split('\n')[-1]
            raise AdvancedAVError(msg)

        return err.decode("utf-8", "replace")

    # ---- Interface ----
    def conv(self, this):
        if not self._prog_conv:
            self._detect_progs()

        args = list(self.generate_args(this))

        return self._exec([self._prog_conv] + args)

    # We depend on english output!
    _probe_env = dict(os.environ)
    _probe_env["LC_ALL"] = _probe_env["LANG"] = "C"

    def probe(self, this, fn, args=[]):
        if fn in this["probes"]:
            return this["probes"][fn]

        if not self._prog_probe:
            self._detect_progs()

        probe = self._exec([self._prog_probe, "-i", fn] + args, self._probe_env)

        this["probes"][fn] = probe

        return probe

    # ---- Setup ----
    _reg_probe_streams = re.compile(r"Stream #0:(?P<id>\d+)(?:\((?P<lang>\w+)\))?: (?P<type>\w+):")

    def probe_streams(self, this, file):
        """ Generate a list of input streams """
        if file in this["instreams"]:
            return this["instreams"][file]

        streams = []

        probe = self.probe(this, file)

        for match in self._reg_probe_streams.finditer(probe):
            stream = match.groupdict()
            streams.append(stream)

        this["instreams"][file] = streams
        return streams

    def add_input(self, this, file):
        """ Add an input file """
        if file in this["inputs"]:
            return this["inputs"].index(file)
        else:
            i = len(this["inputs"])
            self.to_debug("Adding input file #%i: %s", i, file)
            this["inputs"].append(file)
            return i

    @staticmethod
    def stream_type(stream):
        map = {
            "Audio": S_AUDIO,
            "Video": S_VIDEO,
            "Subtitle": S_SUBTITLE
        }

        return map.get(stream["type"], S_UNKNOWN)

    def add_stream(self, this, file, sid):
        """ Add an input stream """
        streams = self.probe_streams(this, file)
        sid = str(sid)

        for stream in streams:
            if stream["id"] == sid:
                break
        else:
            raise AdvancedAVError("Could not add_stream #X:%s of input '%s': No such stream" % (sid, file))

        fid = self.add_input(this, file)
        tid = self.stream_type(stream)
        qid = "%i:%s" % (fid, sid)

        if qid not in this["streams"]["map"]:
            self.to_debug("Adding Stream %s:%i from #%s", tid, len(this["streams"][tid]), qid)
            this["streams"][tid].append(qid)
            this["streams"]["map"].append(qid)

        return tid, qid


    def add_all_streams_from(self, this, file):
        """ Add all input streams """
        streams = self.probe_streams(this, file)

        if not streams:
            self.to_debug("Didn't find any streams in %s", file)

        fid = self.add_input(this, file)
        for stream in streams:
            qid = "%i:%s" % (fid, stream["id"])
            if qid not in this["streams"]["map"]:
                tid = self.stream_type(stream)
                self.to_debug("Adding Stream %s:%i from #%s", tid, len(this["streams"][tid]), qid)
                this["streams"][tid].append(qid)
                this["streams"]["map"].append(qid)

        return fid

    def merge_all(self, this, files):
        """ See FFmpegMergerPP """
        for file in files:
            self.add_all_streams_from(this, file)

    def add_metadata(self, this, info):
        """ Add Metadata, see FFmpegMetadataPP """
        meta = this["metadata"][None]

        def set_meta(key, value):
            meta.append("%s=%s" % (key, value))

        if info.get("title") is not None:
            set_meta("title", info["title"])

        if info.get("upload_date") is not None:
            set_meta("date", info["upload_date"])

        if info.get("uploader") is not None:
            set_meta("artist", info["uploader"])
        elif info.get("uploader_id") is not None:
            set_meta("author", info["uploader_id"])

        if info.get("webpage_url") is not None:
            set_meta("url", info["webpage_url"])

        if info.get("description") is not None:
            set_meta("description", info["description"])

        set_meta("encoded_by", "youtube-dl %s" % __version__)
        set_meta("copyright", "the original artist")

    def embed_subs(self, this, information):
        # Get default sub format
        if self._opts.get("subs_codec"):
            sub_codec = self._opts["subs_codec"].lower()
        else:
            sub_codec = "srt"

        input_sub_codec = sub_codec

        # Check compatibility
        if this["container"].lower() in ("mkv", "webm"):
            if sub_codec not in ("ass", "ssa", "srt"):
                self.to_screen("MKV container cannot handle %s subtitles. Converting to SSA" % sub_codec.upper())
                sub_codec = "ssa"
            else:
                sub_codec = "copy"
        elif this["container"].lower() in ("mp4", "m4v", "mov"):
            if sub_codec != "mov_text":
                self.to_screen("MP4 container support for subtitles is bad. Trying to add subtitles as MOV_TEXT")
                sub_codec = "mov_text"
        else:
            raise AdvancedAVError("Embedding subtitles is not supported for %s containers!" % this["container"].upper())

        # TODO: Make the IE choose the best subtitle codec?

        # Set subtitle codec
        this["codecs"]["s"] = sub_codec

        # Add subtitle streams
        for lang in information['subtitles'].keys():
            fid = self.add_all_streams_from(this, subtitles_filename(information["filepath"], lang, input_sub_codec))

            substreams = [i for i, k in enumerate(this["streams"][S_SUBTITLE]) if k.startswith("%i:" % fid)]

            lang_code = convert_lang_code(lang)
            if lang_code is not None:
                for substream in substreams:
                    this["metadata"]["s:s:%i" % substream].append("language=%s" % lang_code)

    # ---- Commandline  ----
    def argv_inputs(self, inputs):
        for input in inputs:
            yield "-i"
            input = encodeFilename(input, True)
            if input[0] == '-':
                input = "./" + input
            yield input

    def argv_map(self, streams):
        for stream in streams:
            yield "-map"
            yield stream

    def argv_metadata(self, meta):
        for dest, data in meta.items():
            if dest is None:
                opt = "-metadata"
            else:
                opt = "-metadata:%s" % dest
            for meta in data:
                yield opt
                yield meta

    def argv_codecs(self, codecs):
        for dest, codec in codecs.items():
            if dest is None:
                yield "-c"
            else:
                yield "-c:%s" % dest
            yield codec

    def generate_args(self, this):
        """ Turn the this dictionary into a avconv commandline """
        # Arguments
        yield from this["args"]

        # Inputs
        yield from self.argv_inputs(this["inputs"])

        # Streams
        #yield from self.argv_map(this["streams"]["map"])
        yield from self.argv_map(this["streams"][S_VIDEO])
        yield from self.argv_map(this["streams"][S_AUDIO])
        yield from self.argv_map(this["streams"][S_SUBTITLE])

        # Metadata
        yield from self.argv_metadata(this["metadata"])

        # Codecs
        yield from self.argv_codecs(this["codecs"])

        # Container
        yield "-f"
        container = this["container"]
        # the Matroska format is called "matroska", not "mkv", which is the file extension only.
        if container == "mkv":
            container = "matroska"
        yield container

        # Output Filename
        out_fn = encodeFilename(this["output"])
        if out_fn[0] == "-":
            out_fn = "./" + out_fn
        yield out_fn

    # ---- Main ----
    default_args = ["-y"]

    def run(self, information):
        """ PostProcessor entrypoint """
        this = {
            # Output
            "inputs": [],
            "output": None,
            "streams": {
                S_AUDIO: [],
                S_VIDEO: [],
                S_SUBTITLE: [],
                S_UNKNOWN: [],
                "map": []
            },
            "args": list(self.default_args),
            "metadata": collections.defaultdict(list),
            "codecs": {
                None: "copy",
            },
            "container": information["ext"],
            # Input information
            "probes": {},
            "instreams": {},
        }

        task_list = []

        # Merging
        if "__files_to_merge" in information:
            self.merge_all(this, information["__files_to_merge"])
            del information["__files_to_merge"]
            task_list.append("merge")
        else:
            # Add main video file
            self.add_all_streams_from(this, information["filepath"])

        # Metadata
        if self._opts.get("add_metadata"):
            self.add_metadata(this, information)
            task_list.append("add metadata")

        # Repacking
        if self._opts.get("repack_container"):
            this["container"] = information["ext"] = self._opts["repack_container"]
            task_list.append("repack")

        filename = self._downloader.prepare_filename(information) # We want to integrate the new %(ext) properly.
        this["output"] = filename + ".aavtemp"

        # Embed subtitles
        if self._opts.get("embed_subs"):
            self.embed_subs(this, information)
            task_list.append("embed subtitles")

        # Run
        self.to_screen("Running AAV tasks for '%s' (%s)...", filename, ", ".join(task_list))
        self.conv(this)

        # Clean up files
        if self._opts.get("keep_originals"):
            if filename in this["inputs"]:
                os.rename(filename, prepend_extension(filename, "orig"))
        else:
            for file in this["inputs"]:
                os.remove(file)
        os.rename(this["output"], filename)

        # Export information
        information["filepath"] = filename
        information["filename"] = filename.rsplit(u'/', 1)[-1]

        # NOTE: Should it return False?
        return True, information


# ---- Language Map ----
# Taken from the FFmpeg subtitle postprocessor
# See http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
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

def convert_lang_code(code):
    """Convert language code from ISO 639-1 to ISO 639-2/T"""
    return _lang_map.get(code[:2])
