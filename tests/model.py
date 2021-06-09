from eventual.model import Event


class SomethingHappened(Event):
    content: str = "something"
