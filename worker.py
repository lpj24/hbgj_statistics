import requests
import urllib2
import time
import threading
import Queue


if __name__ == "__main__":
    hosts = ["https://www.baidu.com", "http://www.sina.com", "http://weibo.com", "http://apple.com"]

    start = time.time()
    # grabs urls of hosts and prints fir
    #
    #
    #
    #    st 1024 bytes of page
    for host in hosts:
        print host
        html = requests.get(host)

    print "Elapsed Time: %s" % (time.time() - start)

    queue = Queue.Queue()

    class ThreadUrl(threading.Thread):
        """Threaded Url Grab"""

        def __init__(self, queue):
            threading.Thread.__init__(self)
            self.queue = queue

        def run(self):
            while True:
                # grabs host from queue
                host = self.queue.get()
                # grabs urls of hosts and prints first 1024 bytes of page
                print host
                html = requests.get(host)

                # signals to queue job is done
                self.queue.task_done()
    start = time.time()
    for host in hosts:
        queue.put(host)

    for i in xrange(4):
        t = ThreadUrl(queue)
        t.setDaemon(True)
        t.start()

    queue.join()
    print "Elapsed Time: %s" % (time.time() - start)
