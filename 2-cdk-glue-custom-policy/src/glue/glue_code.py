import sys
from awsglue.utils import getResolvedOptions
# import time
# time.sleep(65)


print(sys.argv)
args_list=["fname","lname"]

args = getResolvedOptions(sys.argv,args_list)
print(type(args))
print(args)

print(args["fname"])
print(args["lname"])

