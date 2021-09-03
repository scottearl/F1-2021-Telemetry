import asyncio
import json
import logging
import websockets

import  socket
import  struct  

import threading

def  DecodeDataMessage(message):
    
    values  =  struct.unpack("<HfffcBh",message[0:18])
    print(values)
    return  values
    
def  DecodePacket(data):
    #  Packet  consists  of  5  byte  header  and  multiple  messages.  
    valuesout  =  {}
    headerlen  =  24
    header  =  data[0:headerlen]
    messages  =  data[headerlen:]
    headerData = struct.unpack('<HBBBBdfIBB',header)
    if(headerData[4]==6):
        values  =  DecodeDataMessage(messages)
        valuesout = values
        TELEMETRY["SPEED"] = valuesout[0]
        TELEMETRY["THROTTLE"] = valuesout[1]
        TELEMETRY["STEER"] = valuesout[2]
        TELEMETRY["BRAKE"] = valuesout[3]
        TELEMETRY["GEAR"] = valuesout[5]
        TELEMETRY["RPM"] = valuesout[6]
    
    
def  main():

    #  Open  a  Socket  on  UDP  Port  49000
    UDP_IP  =  ""
    sock  =  socket.socket(socket.AF_INET,  #  Internet
                                              socket.SOCK_DGRAM)  #  UDP
    sock.bind((UDP_IP,  UDP_PORT))

    while  True:
        #  Receive  a  packet
        data,  addr  =  sock.recvfrom(2048)  #  buffer  size  is  1024  bytes
        
        #  Decode  the  packet.  Result  is  a  python  dict  (like  a  map  in  C)  with  values  from  X-Plane.
        #  Example:
        #  {'latitude':  47.72798156738281,  'longitude':  12.434000015258789,  
        #      'altitude  MSL':  1822.67,  'altitude  AGL':  0.17,  'speed':  4.11,  
        #      'roll':  1.05,  'pitch':  -4.38,  'heading':  275.43,  'heading2':  271.84}
        DecodePacket(data)
        

UDP_PORT  =  20777

TELEMETRY = {"SPEED":0,"THROTTLE":0,"STEER":0,"BRAKE":0,"GEAR":0,"RPM":0}

USERS = set()

def TELEMETRY_event():
    return json.dumps({"type": "TELEMETRY", **TELEMETRY})


def users_event():
    return json.dumps({"type": "users", "count": len(USERS)})


async def notify_TELEMETRY():
    if USERS:  # asyncio.wait doesn't accept an empty list
        message = TELEMETRY_event()
        await asyncio.wait([user.send(message) for user in USERS])


async def notify_users():
    if USERS:  # asyncio.wait doesn't accept an empty list
        message = users_event()
        await asyncio.wait([user.send(message) for user in USERS])


async def register(websocket):
    USERS.add(websocket)
    await notify_users()


async def unregister(websocket):
    USERS.remove(websocket)
    await notify_users()


async def counter(websocket, path):
    # register(websocket) sends user_event() to websocket
    await register(websocket)
    try:
        await websocket.send(TELEMETRY_event())
        async for message in websocket:
            data = json.loads(message)
            if data["action"] == "TELEMETRY":
                await notify_TELEMETRY()
            else:
                logging.error("unsupported event: {}", data)
    finally:
        await unregister(websocket)


threading.Thread(target=main).start()
start_server = websockets.serve(counter, "localhost", 6789)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()



