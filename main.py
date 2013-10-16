import socket, select, struct, binascii

# MySQL command codes
MYSQL_SLEEP               = 0  # not from client */
MYSQL_QUIT                = 1
MYSQL_INIT_DB             = 2
MYSQL_QUERY               = 3
MYSQL_FIELD_LIST          = 4
MYSQL_CREATE_DB           = 5
MYSQL_DROP_DB             = 6
MYSQL_REFRESH             = 7
MYSQL_SHUTDOWN            = 8
MYSQL_STATISTICS          = 9
MYSQL_PROCESS_INFO        = 10
MYSQL_CONNECT             = 11 # not from client */
MYSQL_PROCESS_KILL        = 12
MYSQL_DEBUG               = 13
MYSQL_PING                = 14
MYSQL_TIME                = 15 # not from client */
MYSQL_DELAY_INSERT        = 16 # not from client */
MYSQL_CHANGE_USER         = 17
MYSQL_BINLOG_DUMP         = 18 # replication */
MYSQL_TABLE_DUMP          = 19 # replication */
MYSQL_CONNECT_OUT         = 20 # replication */
MYSQL_REGISTER_SLAVE      = 21 # replication */
MYSQL_STMT_PREPARE        = 22
MYSQL_STMT_EXECUTE        = 23
MYSQL_STMT_SEND_LONG_DATA = 24
MYSQL_STMT_CLOSE          = 25
MYSQL_STMT_RESET          = 26
MYSQL_SET_OPTION          = 27
MYSQL_STMT_FETCH          = 28

DEBUG = False

class Packet:
    def __init__(self):
        self.content = ''
        return

    def Clear(self):
        self.content = ''
        return

    def Init(self, data):
        self.content = data
        self.GetUint32()
        return

    def uint8(self, u):
        self.content = self.content + struct.pack('!B', u)
        return

    def uint16(self, u):
        self.content = self.content + struct.pack('<H', u)
        return

    def uint32(self, u):
        self.content = self.content + struct.pack('<I', u)
        return

    def string(self, s, nul = False, setlen = False):
        if (setlen == True):
            self.content = self.content + chr(len(s))
        self.content = self.content + s
        if (nul == True):
            self.content = self.content + chr(00)
        return

    def HexBlob(self, blob):
        blob = blob.split(' ')
        for data in blob:
            self.content = self.content + chr(int(data.strip(), 16))
        return

    def GetPacket(self, seqnumber = 0, withoutHead = False):
        self.out = ''
        if (withoutHead == True):
            self.out = self.content
        if (len(self.out) < 1):
            self.out = struct.pack('<I', len(self.content))
            self.out = self.out[0:3]
            self.out = self.out + struct.pack('!B', seqnumber) + self.content

        if (DEBUG == True):
            print "SEND: " + self.out
            print "SEND: " + binascii.hexlify(self.out)
        return self.out

    #OutPut
    def GetUint8(self):
        self.out = self.content[0]
        self.content = self.content[1:]
        return struct.unpack('!B', self.out)[0]

    def GetUint16(self):
        self.out = ''
        for data in range(0,2):
            self.out = self.out + self.content[data]
        self.content = self.content[2:]
        return struct.unpack('!H', self.out)

    def GetUint32(self):
        self.out = ''
        for data in range(0,4):
            self.out = self.out + self.content[data]
        self.content = self.content[4:]
        return struct.unpack('!I', self.out)

    def GetString(self, skipNULL= False, termNULL = False, len=0):
        self.out = ''
        count = 0
        for data in self.content:
            count = count + 1
            if (len > 0 and count >= len):
                break
            if (termNULL == True):
                if ord(data) == 0:
                    break
            if (skipNULL == True):
                if ord(data) == 0:
                    continue
            self.out = self.out + data
        self.content = self.content[count:]
        return self.out


class Client:
    def __init__(self, socket):
        self.socket = socket
        self.conInit = False
        self.packet = Packet()

        self.capflags = 0
        self.charSet = 0
        self.sequenceNumber = 0
        self.username = ''
        self.password = ''
        return

    def SendAuth(self):
        self.packet.Clear()
        self.packet.uint8(10)
        self.packet.string("Pudel des Todes", True)
        self.packet.uint32(01) #connectionnumber
        self.packet.string("a-Z`RZOZ")
        self.packet.uint8(00)
        self.packet.HexBlob("ff f7")
        self.packet.uint8(8) #LaenderCode
        self.packet.HexBlob("02 00")
        self.packet.uint16(250) #CapabilityFlags
        self.packet.HexBlob("15 00 00 00 00 00 00 00 00 00 00 76 22 3f 58 7d 2c 62 28 3f 46 21 6f 00 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77 6f 72 64 00")
        self.socket.send(self.packet.GetPacket(00))
        return

    def ReadAuthResponse(self):
        temp = self.packet.GetUint32()  # Skip Capabilities
        temp = self.packet.GetUint32()  # Skip Max Packet Size
        self.charSet = self.packet.GetUint8()
        self.username = self.packet.GetString(True, False, 29)
        temp = self.packet.GetString(False, True)
        self.conInit = True
        self.BuildResponsePacket(2)
        return

    def RecvData(self, data):
        if (DEBUG == True):
            print "rec: " + data
            print "rec: " + repr(data)
            print "rec: " + binascii.hexlify(data)
        self.packet.Init(data)
        if (self.conInit == False):
            self.ReadAuthResponse()
            return
        self.packet.Init(data)
        self.ReadSQL()
        return

    def ReadSQL(self):
        ccm = self.packet.GetUint8()
        if (ccm == MYSQL_PING):
            self.BuildResponsePacket(1)
            return
        if (ccm == MYSQL_QUERY):
            data = self.packet.GetString()
            try:
                command = data.split(' ')[0].strip()
                if (DEBUG == True):
                    print command
                #print command
                handler = getattr(self, 'handle_%s' % (command.lower()), None)
                handler(data)
            except IndexError:
                pass
            return
        return

    def handle_select(self, data):
        idx = data.split(" ")[1]
        self.SendPacket(self.BuildResultSetPacket([idx], ['Pudelserver ohne Funktion', 00]))
        return

    def handle_set(self, data):
        self.BuildResponsePacket(1)
        return

    def handle_show(self, data):
        self.SendPacket(self.BuildResultSetPacket(['Was', 'Ach das'], ['Das', 'Holz', 00, \
                                                                       'Der', 'Hund',  00]))
        return

    #PacketBuildings
    def BuildResponsePacket(self, seq = 0):
        self.packet.Clear()
        self.packet.uint8(0) #OK-Header
        self.packet.uint8(0)
        self.packet.uint8(0)
        self.packet.uint16(2) #StatusFlag
        self.packet.uint16(0) #Warnings
        if (seq <= 0):
            self.SendPacket(self.packet.GetPacket(self.GetSequenceNumber()))
        else:
            self.socket.send(self.packet.GetPacket(seq))
        return

    def BuildResultSetPacket(self, cols, rows):
        fpacket = ''
        packet = Packet()
        packet.uint8(len(cols))
        fpacket = fpacket + packet.GetPacket(self.GetSequenceNumber())
        for Name in cols:
            packet.Clear()
            packet.HexBlob("03 64 65 66 00 00 00")
            packet.string(Name, False, True)
            packet.HexBlob("00 0c")
            packet.uint16(8) #CharSet
            packet.uint32(28) # Spaltenbreite
            packet.HexBlob("fd 00 00 1f")
            packet.uint16(0) #FILL
            fpacket = fpacket + packet.GetPacket(self.GetSequenceNumber())

        fpacket = fpacket + self.BuildEOF()

        packet.Clear()
        for Name in rows:
            if Name == 0:
                fpacket = fpacket + packet.GetPacket(self.GetSequenceNumber())
                packet.Clear()
                continue
            packet.string(Name, False, True)
        fpacket = fpacket + self.BuildEOF()
        return fpacket

    def BuildEOF(self, flag = 2):
        packet = Packet()
        packet.uint8(254) #EOF Indicator
        packet.uint16(00) #WarningCounter
        packet.uint16(flag)  #Status-Flags (SERVER_STATUS_AUTOCOMMIT = 2) 0x0008
        return packet.GetPacket(self.GetSequenceNumber())

    def SendPacket(self, packet):
        self.sequenceNumber = 0
        self.socket.send(packet)
        return

    def GetSequenceNumber(self):
        self.sequenceNumber = self.sequenceNumber + 1
        return self.sequenceNumber


if __name__ == "__main__":

    # List to keep track of socket descriptors
    CONNECTION_LIST = []
    CLIENT_LIST = []
    RECV_BUFFER = 1024 # Advisable to keep it as an exponent of 2
    PORT = 3307

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # this has no effect, why ?
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", PORT))
    server_socket.listen(10)

    # Add server socket to the list of readable connections
    CONNECTION_LIST.append(server_socket)

    print "Chat server started on port " + str(PORT)

    while 1:
        # Get the list sockets which are ready to be read through select
        read_sockets, write_sockets, error_sockets = select.select(CONNECTION_LIST, [], [])
        for sock in read_sockets:
            #New connection
            if sock == server_socket:
                # Handle the case in which there is a new connection recieved through server_socket
                sockfd, addr = server_socket.accept()
                client = Client(sockfd)
                CONNECTION_LIST.append(sockfd)
                CLIENT_LIST.append(client)
                print "Client (%s, %s) connected" % addr
                client.SendAuth()

            else:
                # Data recieved from client, process it
                try:
                    data = sock.recv(RECV_BUFFER)
                    if data:
                        #print data
                        for client in CLIENT_LIST:
                            if (client.socket == sock):
                                client.RecvData(data)

                except IndexError:
                    print "Client (%s, %s) is offline" % addr
                    sock.close()
                    CONNECTION_LIST.remove(sock)
                    for client in CLIENT_LIST:
                        if (client.socket == sock):
                            CLIENT_LIST.remove(client)
                    continue

server_socket.close()
