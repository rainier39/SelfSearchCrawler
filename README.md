# SelfSearchCrawler
This Python script is my work in progress web crawler for my work in progress search engine (software). It is currently in relatively early development. So far, there are basic measures to respect websites' robots.txt files. There may be bugs with my implementation, however, as well as missing features. The crawler clearly declares itself with its user agent string as being made by me, linking back to this repository. If you are a website owner who found your way here and don't want your site crawled, feel free to open an issue. I cannot control people running instances of this software, however. All I can do is add people's websites to the (default) blacklist file. I can also attempt to fix any bugs with the crawler or its behavior. One can edit their robots.txt file to exclude this crawler as well. I am not responsible for anything users of this software do with it.

Ultimately my intention is to create a self-hosted search engine. This crawler is one half of the project, the other half is the actual PHP webapp ([see here](https://github.com/rainier39/SelfSearch)). For now, this is an incomplete project. The crawler is very basic and is likely far from pefect at this point in time.

## Usage
This is incomplete software so I wouldn't recommend using it currently.

## TLD List
I got the `tlds-alpha-by-domain.txt` file from here: https://data.iana.org/TLD/tlds-alpha-by-domain.txt

## Version Information
Tested on Debian 13 (trixie) with Python 3.13.5 and MariaDB 11.8.3.
