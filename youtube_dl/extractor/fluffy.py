from youtube_dl.extractor.common import InfoExtractor

import re

class FluffyIE(InfoExtractor):
    IE_NAME = "fluffy.is"
    _VALID_URL = r"https?://fluffy\.is/(watch|view)\.php\?id=(?P<id>\d+)"
    
    _src_re = re.compile(r"<param name=\"src\" value=\"([^\"]+)")
    _title_re = re.compile(r"<param name=\"movieTitle\" value=\"([^\"]+)")
    
    def _real_extract(self, url):
        video_id = self._match_id(url)
        
        watch_page = self._download_webpage("http://fluffy.is/watch.php?id=%s" % video_id, video_id)
        
        return {
            "id": video_id,
            "title": self._title_re.search(watch_page).group(1),
            "author": "fluffy.is",
            "url": "http:" + self._src_re.search(watch_page).group(1),
            #"url": "http://fluffy.is/download.php?id=" + video_id,
        }
