from youtube_dl.extractor.common import InfoExtractor

import re
import json


class ProxerStreamIE(InfoExtractor):
    IE_NAME = "stream.proxer.me"
    _VALID_URL = r"https?://stream.proxer.me/embed-(?P<id>[^-]+)-"
    
    _url_re = re.compile(r'''<source type="video/[^"]+" src="([^"]+)"''')
    
    def _real_extract(self, url):
        vid = self._match_id(url)
        
        content = self._download_webpage(url, vid)
        
        return {
            "url": self._url_re.search(content).group(1)
        }


class ProxerIE(InfoExtractor):
    IE_NAME = "proxer.me"
    _VALID_URL = r"https?://proxer.me/watch/(?P<id>\d+/\d+/engsub)"
    
    _streams_re = re.compile(r"var streams = (\[.+\]);")
    _title_re = re.compile(r"<title>(.+) - Proxer.Me</title>")
    
    _stream_types = [ # sorted by preference
        {
            "type": "proxer-stream",
            "ie": "ProxerStream",
            "preference": 100,
        },
        {
            "type": "streamcloud2",
            "ie": "Streamcloud",
            "preference": 10,
            "avoid": True,
        },
        {
            "type": "videoweed",
            "ie": "VideoWeed",
            "preference": 1,
        },
        {
            "type": "novamov",
            "ie": "NovaMov",
            "preference": 1,
        },
    ]
    
    _type_index = {t["type"]: t for t in _stream_types}
    _type_list = [t["type"] for t in _stream_types]
    
    def _real_extract(self, url):
        vid = self._match_id(url)
        
        series, ep, type = vid.split("/")
        content = self._download_webpage(url, vid)
        
        streams = json.loads(self._streams_re.search(content).group(1))
        
        result = {
            "id": vid,
            "title": self._title_re.search(content).group(1),
            "formats": [],
        }
        
        formats = []
        for stream in streams:
            if stream["type"] in self._type_index:
                formats.append((
                    self._type_list.index(stream["type"]),
                    self._type_index[stream["type"]],
                    stream
                ))
            else:
                self._downloader.report_warning("Unknown Proxer Stream Type: %s" % stream["type"])
        
        formats.sort(key=lambda x: x[0])
        
        for order, stype, stream in formats:
            if "avoid" in stype and stype["avoid"] and result["formats"]: # skip if not best available
                self._downloader.report_warning("Avoiding Proxer Stream of type %s" % stream["type"])
            else:
                if "func" in stype:
                    t["func"](self, t, stream, result)
                elif "ie" in stype:
                    ie = self._downloader.get_info_extractor(stype["ie"])
                    url = stream["replace"].replace("\\/", "/").replace("#", stream["code"])
                    self._downloader.to_screen("Proxer %s stream: %s" % (stream["type"], url))
                    try:
                        sres = ie.extract(url)
                    except:
                        import traceback
                        self._downloader.report_warning("In %s Extractor for proxer stream:\n" % stream["type"] + "".join(traceback.format_exc()))
                    else:
                        sres.update({
                            "format_note": stream["name"],
                            "preference": stype["preference"] if "preference" in stype else 0,
                        })
                        result["formats"].append(sres)
        
        result["formats"].reverse() # youtube-dl likes best quality last
        
        return result
