import contextvars
import threading

# 用来存储每个请求的ID，协程相关
request_id = contextvars.ContextVar('request_id')


def get_context_id():
    """
    获取上下文ID
    如果在tornado请求中（ SessionMixin 会在 prepare 中设置request_id ）
    则返回设置的 request_id, 否则返回线程的唯一标识
    :return:
    """
    try:
        return 'request-' + request_id.get()
    except LookupError:
        return 'thread-' + str(threading.get_ident())


def set_request_id(_id):
    """
    设置 request_id
    """
    request_id.set(_id)


def get_request_id():
    """
    获取 request_id
    如果在tornado请求中（ SessionMixin 会在 prepare 设置request_id ）
    则返回设置的 request_id, 否则返回 ''
    :return:
    """
    try:
        return request_id.get()
    except LookupError:
        return ''
