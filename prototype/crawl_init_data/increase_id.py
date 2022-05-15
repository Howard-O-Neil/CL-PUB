def check_full_z(name):
    for character in name:
        if character != "z":
            return False
    return True

def next_letter(s: str):
    if (s >= "a" and s <= "z") or (s >= "A" and s <= "Z"):
        return chr((ord(s.upper()) + 1 - 65) % 26 + 65).lower()
    else:
        raise Exception("Not an alphabet")

def cal_next_id(s: str):
    s = list(s.lower())

    if check_full_z(s):
        s.append("a")
    else: s[-1] = next_letter(s[-1])

    return "".join(s)

if __name__ == "__main__":
    test_strs = [
        "a",
        "zzz",
        "zz",
        "zb",
        "zzzc",
        "e",
        "zzZ",
        "B"
    ]
    for id in test_strs:
        print(cal_next_id(id) + "EOF") # Check if there are any space