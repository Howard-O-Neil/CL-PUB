import sys
import typing
from typing import NewType

UserId = NewType('UserId', int)

sys.setrecursionlimit(sys.getrecursionlimit() * 1000)

def original_gridTraveler(m: typing.SupportsInt, n: typing.SupportsInt):
    if m == 1 and n == 1: return 1
    if m == 0 or n == 0: return 0

    return original_gridTraveler(m - 1, n) + original_gridTraveler(m, n - 1)

def gridTraveler(m: typing.SupportsInt, n: typing.SupportsInt, memo: dict):
    if m == 1 and n == 1: return 1
    if m == 0 or n == 0: return 0

    if m in memo and n in memo[m]:
        return memo[m][n]

    memo[m] = {}
    memo[m][n] = gridTraveler(m - 1, n, memo) + gridTraveler(m, n - 1, memo)
    return memo[m][n]


print(gridTraveler(2, 3, {}))
    
