import time
import signal


def test(i):
    time.sleep(i%4)
    print "%d within time"%(i)
    return i

if __name__ == '__main__':
    def handler(signum, frame):
        exit()

    i = 0
    for i in range(1,10):
        try:
            signal.signal(signal.SIGINT, handler)
            signal.alarm(3)
            test(i)
            i = i + 1
            signal.alarm(0)
        except AssertionError:
            break
