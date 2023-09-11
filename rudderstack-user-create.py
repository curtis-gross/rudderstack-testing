#make sure the following packages are installed.
import rudderstack.analytics as rudder_analytics #need to install package, currently this script is not using the sdk and is using the HTTP API
import rudderstack_keys
import requests #need to install package
import json
import calendar
import random
import datetime #need to install package
from datetime import datetime
import time
from faker import Faker  #need to install package
from itertools import count
fake = Faker()
known = 0
setting = {}

# If you are using the Rudderstack SDK for python
# rudder_analytics.write_key = "write key from the js sdk when you add it in rudderstack"
# rudder_analytics.dataPlaneUrl = "https://EXAMPLE.dataplane.rudderstack.com"

basic_key = rudderstack_keys.basic_key
data_plane_url = rudderstack_keys.data_plane_url
#create a python file called 'rudderstack_keys.py' and put a basic_key = "" and data_plane_url = ""
# into the file in the same folder as this file
# You generate the basic key using the documentation https://www.rudderstack.com/docs/api/http-api/ and
# to generate the 'long' basic auth go here:  https://www.blitter.se/utils/basic-authentication-header-generator/

#data plane is the url to your rudderstack account. This is listed on the main dashboard landing page.
data_plane_url = "https://rudderstacvmg.dataplane.rudderstack.com"


timetosleep = 1 #time to sleep between each user creation event.
########
#users_to_create = int(input("How many users_to_create do you want to create? "))
users_to_create = 50

headers = {
        'Authorization': "{}".format(basic_key),
        'Content-Type': "application/json",
        'cache-control': "no-cache"
        }

def increment_known():
    global known
    known += 1

def runner(): #the runner can be modified to be multi-threaded, currently single threaded for simplicity
    starting_uid = 1
    print("Starting User Creation at ",starting_uid)
    upper_bound = users_to_create + starting_uid
    user_range = list(range(starting_uid, upper_bound))
    for user in user_range:
        create_user(user)

#this code is the actual user creation script that is run multiple times by the 'runner'
def create_user(current_user):   
    data = {}
    context = {}
    anon = {}
    account = ''
    account = fake.ean(length=13)
    uid = current_user

    #General web Data
    timestamp = datetime.now().isoformat()              
    full_name = fake.name() #you could use transformations to modify this value (in rudderstack)
    FLname = full_name.split(" ")
    Fname = FLname[0]
    Lname = FLname[1]
    name = Fname +" "+ Lname 
    domain_name = "@demodomain.com"
    email_address = Fname + "." + Lname + domain_name #email as a subset of name
    email_address = email_address.lower()
    #parameters that are already mapped to user profiles
    x = 2
    
    #The identify endpoint for rudderstack is what is supported by redis as a destination, so that is the endpoint being used here.
    #the following builds the json that is sent to the rudderstack api. The event data is duplicated because the audience api doesn't have an
    #option to grab the event / event properties from Big Query (what I was testing) so I sent it as a trait as well.
    identify = {
      "userId": "{}".format(email_address),
      "type": "identify",
      "anonymousId": (fake.ean(length=13)),
      "channel": "{}".format(random.choice(['web', 'mobile', 'server', 'sources'])),
      "event": "{}".format(random.choice(['download', 'purchase','registered'])),
      }
    if identify['event'] == "purchase":
        identify.update({
           "properties": {
               "product_name": "{}".format(random.choice(['Oreos','Bread','Steak'])),
                "revenue": "{}".format(random.randint(1,50))
            },})
        identify.update({
          "context": {
                "traits": {
                   "browsertype": "{}".format(random.choice(['Android', 'Mac', 'iOS', 'Windows', 'Linux'])),
                   "name": "{}".format(name),
                   "phone": "{}".format(fake.phone_number()),
                   "city": "{}".format(fake.city()),
                   "country": "{}".format(fake.country()),
                   "address": "{}".format(fake.address()),
                   "zipcode": "{}".format(fake.zipcode()),
                   "event_type": "{}".format(identify['event']),
                   "product_name": "{}".format(random.choice(['Oreos','Bread','Steak'])),
                   "revenue": "{}".format(random.randint(1,50))
                   }
                }})        
        
    elif identify['event'] == "download":
        identify.update({
           "properties": {
               "download_name": "{}".format(random.choice(['whitepaper','customer story','video']))       
            },})
        identify.update({
          "context": {
                "traits": {
                   "browsertype": "{}".format(random.choice(['Android', 'Mac', 'iOS', 'Windows', 'Linux'])),
                   "name": "{}".format(name),
                   "phone": "{}".format(fake.phone_number()),
                   "city": "{}".format(fake.city()),
                   "country": "{}".format(fake.country()),
                   "address": "{}".format(fake.address()),
                   "zipcode": "{}".format(fake.zipcode()),
                   "event_type": "{}".format(identify['event']),
                   "download_name": "{}".format(random.choice(['whitepaper','customer story','video']))
                   }
                }})
    elif identify['event'] == "registered":
        identify.update({
           "properties": {
               "form_name": "{}".format(random.choice(['main page','free trial','sales contact']))       
            },})
        identify.update({
          "context": {
                "traits": {
                   "browsertype": "{}".format(random.choice(['Android', 'Mac', 'iOS', 'Windows', 'Linux'])),
                   "name": "{}".format(name),
                   "phone": "{}".format(fake.phone_number()),
                   "city": "{}".format(fake.city()),
                   "country": "{}".format(fake.country()),
                   "address": "{}".format(fake.address()),
                   "zipcode": "{}".format(fake.zipcode()),
                   "event_type": "{}".format(identify['event']),
                   "form_name": "{}".format(random.choice(['main page','free trial','sales contact']))
                   }
                }})        
  #round out the end of the json package.     
    identify.update({
        "ip": "{}".format(fake.ipv4()),
           "library": {
             "name": "http"
            }
        })                
    identify.update({"originalTimestamp": "{}".format(timestamp)})
    identify.update({"sentAt": "{}".format(timestamp)})
  
    
    
    print("-----Creating Known user", uid, email_address,"-----")
    trackurl = data_plane_url + "/v1/identify"                
    json_data = json.dumps(identify)
    print(json_data) #print the json being sent to the API

#     rudder_analytics.identify(json_data) #if you are using the js sdk and rudder sdk, you can uncomment this line.
    request = requests.post(trackurl, data=json_data, headers=headers)
    responsetext = request.text #convert to text for formatting
    print(responsetext) #proper response should be "OK" from the rudder http api
    print("sleeping ",timetosleep," seconds")
    
    time.sleep(timetosleep) #wait a defined time (in seconds) before runnning again.  Adjust at top of script.
    increment_known()
        

if __name__ == '__main__':
     runner() #main thread runs the 'runner' until total number of users that are to be created is reached.
else:
    print("some sort of error?", file = log_file)

print("script completed")