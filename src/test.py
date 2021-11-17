# using asterisk
def food(**kwargs):
  for items in kwargs:
    print(f"{kwargs[items]} is a {items}")
      
dict = {'fruit' : 'cherry', 'vegetable' : 'potato', 'boy' : 'srikrishna'}
# using asterisk
food(**dict)