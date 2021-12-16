import sys
sys.setrecursionlimit(sys.getrecursionlimit() * 1000)

def original_fib(n):
    if n <= 2: return 1
    return original_fib(n - 1) + original_fib(n - 2)

def fib(n, memo: dict):
    if n in memo:
        return memo[n]
    if n <= 2: return 1

    memo[n] = fib(n - 1, memo) + fib(n - 2, memo)
    return memo[n]


l, n = {}, 1000

# much faster
print(fib(n, l))

# much slower
print(original_fib(n))
