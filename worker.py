# from rq import Connection, Queue
# from redis import Redis
# import time
# from job import count_words_at_url
#
#
# if __name__ == "__main__":
#     redis_conn = Redis()
#     q = Queue(connection=redis_conn)
#     jobs = q.enqueue(count_words_at_url, "http://nvie.com")
#     print jobs.result
#     time.sleep(2)
#     print jobs.result

import click


@click.command()
@click.option('--name', prompt='Your name', help='The person to greet.')
@click.option('--gender', default='debug', type=click.Choice(["debug", "product"]), help='chose environment')
def hello(name, gender):
    click.secho("Hello %s, %s " % (name, gender), fg='red')

if __name__ == "__main__":
    hello()