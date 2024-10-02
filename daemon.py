import threading


def run(target, *args, **kwargs):
    threading.Thread(
        target=target,
        args=args,
        kwargs=kwargs,
        daemon=True,
    ).start()


def prepare_thread(target, *args, **kwargs) -> threading.Thread:
    return threading.Thread(
        target=target,
        args=args,
        kwargs=kwargs,
        daemon=True,
    )
