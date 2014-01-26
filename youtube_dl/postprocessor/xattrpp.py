import os
import subprocess
import sys

from .common import PostProcessor
from ..utils import (
    check_executable,
    hyphenate_date,
)

# Preemtively check FS support
def getmount(path):
        path = os.path.realpath(os.path.abspath(path))
        while path != os.path.sep:
            if os.path.ismount(path):
                return path
            else:
                path = os.path.abspath(os.path.join(path, os.pardir))
        return path

if os.path.exists("/proc/mounts"):
    def getfs(path):
        mount = getmount(path)
        with open("/proc/mounts") as mtab:
            for line in mtab:
                dev, mp, fs = line.split()[:3]
                if mp == mount:
                    return fs
else:
    def getfs(path):
        return None

def fs_supports_long_xattrs(fs):
    """ True = arbitary, False = all values must fit into a block, None = unknown """
    if fs.startswith("ext"):
        return False
    elif os.name == "nt":
        return True # NTFS ADS' are arbitary


class XAttrMetadataPP(PostProcessor):

    #
    # More info about extended attributes for media:
    #   http://freedesktop.org/wiki/CommonExtendedAttributes/
    #   http://www.freedesktop.org/wiki/PhreedomDraft/
    #   http://dublincore.org/documents/usageguide/elements.shtml
    #
    # TODO:
    #  * capture youtube keywords and put them in 'user.dublincore.subject' (comma-separated)
    #  * figure out which xattrs can be used for 'duration', 'thumbnail', 'resolution'
    #  * Handle the filesystem mess more elegantly
    #

    def run(self, info):
        """ Set extended attributes on downloaded file (if xattr support is found). """

        # Write the metadata to the file's xattrs
        self._downloader.to_screen('[metadata] Writing metadata to file\'s xattrs')

        filename = info['filepath']

        # This mess below finds the best xattr tool for the job and creates a
        # "write_xattr" function.
        try:
            # try the pyxattr module...
            import xattr
            import errno

            def write_xattr(path, key, value):
                try:
                    return xattr.setxattr(path, key, value)
                except OSError as e:
                    if e.errno == errno.ENOSPC:
                        self._downloader.report_warning("Extended attributes exceed reserved file system space. "
                                                        "Some file systems try to fit all attributes into a single block. "
                                                        "Skipping %s (%i bytes)" % (key, len(value)))
                        return False
                    else:
                        raise

        except ImportError:
            if os.name == 'nt':
                # Write xattrs to NTFS Alternate Data Streams:
                # http://en.wikipedia.org/wiki/NTFS#Alternate_data_streams_.28ADS.29
                def write_xattr(path, key, value):
                    assert ':' not in key
                    assert os.path.exists(path)

                    ads_fn = path + ":" + key
                    with open(ads_fn, "wb") as f:
                        f.write(value)
            else:
                user_has_setfattr = check_executable("setfattr", ['--version'])
                user_has_xattr = check_executable("xattr", ['-h'])

                if user_has_setfattr or user_has_xattr:

                    def write_xattr(path, key, value):
                        if user_has_setfattr:
                            cmd = ['setfattr', '-n', key, '-v', value, path]
                        elif user_has_xattr:
                            cmd = ['xattr', '-w', key, value, path]

                        subprocess.check_output(cmd)

                else:
                    # On Unix, and can't find pyxattr, setfattr, or xattr.
                    if sys.platform.startswith('linux'):
                        self._downloader.report_error(
                            "Couldn't find a tool to set the xattrs. "
                            "Install either the python 'pyxattr' or 'xattr' "
                            "modules, or the GNU 'attr' package "
                            "(which contains the 'setfattr' tool).")
                    else:
                        self._downloader.report_error(
                            "Couldn't find a tool to set the xattrs. "
                            "Install either the python 'xattr' module, "
                            "or the 'xattr' binary.")
                    return False, info

                # Protect from too long values
                file_system = getfs(filename)
                long_values = fs_supports_long_xattrs(file_system)

                if long_values is not True and hasattr(os, "statvfs"):
                    _real_write_xattr = write_xattr
                    blocksize = os.statvfs(filename).f_bsize
                    written = []

                    if long_values is False:
                        def write_xattr(filename, xattr, value):
                            written.append(len(value))
                            total = sum(written)
                            if total > blocksize - 256:
                                self._downloader.report_warning(("XAttr size approaches blocksize (%iB/%iB). "
                                                                 "Your file system (%s) doesn't support longer extended attributes. "
                                                                 "Skipping %s (%iB)") %
                                                                (total, blocksize, file_system, xattr, written[-1]))
                                written.pop()
                                return False
                            else:
                                _real_write_xattr(filename, xattr, value)
                    else:
                        def write_xattr(filename, xattr, value):
                            written.append(len(value))
                            total = sum(written)
                            if total > blocksize - 256:
                                try:
                                    _real_write_xattr(filename, xattr, value)
                                except (subprocess.CalledProcessError, OSError):
                                    self._downloader.report_warning(("XAttr size approaches blocksize (%iB/%iB). "
                                                                     "Your file system%s might not support longer extended attributes. "
                                                                     "Skipping %s (%iB)") %
                                                                    (total, blocksize, " (%s)" % file_system if file_system is not None else "", xattr, written[-1]))
                                    written.pop()
                                    return False
                            else:
                                _real_write_xattr(filename, xattr, value)

        try:
            # Order is important here! Do description last because it's most likely to kill the blocksize boundary.
            xattr_mapping = [
                ('user.xdg.referrer.url', 'webpage_url'),
                ('user.dublincore.title', 'title'),
                ('user.dublincore.contributor', 'uploader'),
                ('user.dublincore.date', 'upload_date'),
                ('user.dublincore.format', 'format'),
                ('user.dublincore.description', 'description'),
                #('user.xdg.comment', 'description'),
            ]

            for xattrname, infoname in xattr_mapping:

                value = info.get(infoname)

                if value:
                    if infoname == "upload_date":
                        value = hyphenate_date(value)

                    byte_value = value.encode('utf-8')
                    write_xattr(filename, xattrname, byte_value)

            return True, info

        except (subprocess.CalledProcessError, OSError):
            self._downloader.report_error("This filesystem doesn't support extended attributes. (You may have to enable them in your /etc/fstab)")
            return False, info

