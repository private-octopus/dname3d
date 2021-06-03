# dname3d
Analysis of DNS name clusters

The first step in the analysis is to look at the [public suffix list](https://github.com/publicsuffix/list),
which describes suffixes like ".com" or ".co.uk". It is very useful for web browsers, because of the web
security policy based on "origin domain". For example, a script on page "www.example.com" can load a file
from "img.example.com" (same origin) but not from "data.example.net" (different origin).
We would not want a script on "special-projects.myspotify.com" to read data
from "julie-s-flower-shop.myspotify.com", but the only way to achieve that is to declare "myspotify.com"
as a public suffix, and add it to the list. Turns out that there are quite a few domains like "myspotify.com",
and adding all of them is feeding a burst of growth in the list.

The analysis is performed by the script `sublist.py`, found in the `stats` folder.
