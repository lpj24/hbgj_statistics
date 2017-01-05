import requests


def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())

if __name__ == "__main__":
    print count_words_at_url("http://www.cnblogs.com/gudaojuanma/p/Python-RQ-Job.html")
