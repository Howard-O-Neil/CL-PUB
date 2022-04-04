from abc import ABC, abstractmethod
from typing import Any


class test_1:
    def __init__(self) -> None:
        pass

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        self.call(*args)
        pass
    
    @abstractmethod
    def call(self, a, b):
        raise NotImplementedError()


class test_2(test_1):
    def __init__(self) -> None:
        super(test_2).__init__()
    
    def call(self, a, b):
        print(a + b)

test = test_2()
test(1, 2)