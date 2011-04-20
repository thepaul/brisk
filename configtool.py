#!/usr/bin/env python

import re, sys
from optparse import OptionParser

confPath = 'resources/cassandra/conf/'
hconfPath = 'resources/hadoop/conf/'
opsConfPath = '/etc/opscenter/'

clusterName = ''
seedList = ''
clusterSize = False
autoBootstrap = False
internalIP = ''
tokenPosition = -1

DEBUG = True

def promptUserInfo():
    global clusterName, seedList, clusterSize, autoBootstrap, internalIP, tokenPosition
    clusterName = raw_input("Cluster name:\n")
    
    seedList = raw_input("Seed list (comma-delimited):\n")
    seedList = seedList.replace(' ', '')
    seedList = seedList.split(',')
    seedList.sort()

    while (not type(clusterSize) is int):
        clusterSize = raw_input("Current cluster size:\n")
        try:
            clusterSize = int(clusterSize)
        except:
            print "Please enter a valid number."

    while not (autoBootstrap == 'y' or autoBootstrap == 'n'):
        autoBootstrap = raw_input("Is this node being autoBootStrapped? [y/n]\n").strip()
    if autoBootstrap == 'y':
        autoBootstrap = 'true'
    else:
        autoBootstrap = 'false'

    internalIP = raw_input("This node's internal IP address:\n")
    
    while (tokenPosition < 0) and (tokenPosition >= clusterSize):
        tokenPosition = raw_input("This node's token position (position < cluster size):\n")
        try:
            tokenPosition = int(tokenPosition)
        except:
            print "Please enter a valid number."

def commandLineSwitches():
    global clusterName, seedList, clusterSize, autoBootstrap, internalIP, tokenPosition, confPath, hconfPath, opsConfPath
    parser = OptionParser()
    parser.add_option("-n", "--clusterName", action="store", type="string", dest="clusterName", help="Set the cluster name.")
    parser.add_option("-l", "--seedList", action="store", type="string", dest="seedList", help="Provide a comma-delimited list of seeds.")
    parser.add_option("-s", "--clusterSize", action="store", type="int", dest="clusterSize", help="Provide the size of the cluster for equal partitioning.")
    parser.add_option("-a", "--autoBootstrap", action="store_true", dest="autoBootstrap", help="Set autobootstrap to true. (Defaults to false.)")
    parser.add_option("-i", "--internalIP", action="store", type="string", dest="internalIP", help="Set this nodes internal ip address.")
    parser.add_option("-t", "--tokenPosition", action="store", type="int", dest="tokenPosition", help="Set this node's token position.")


    parser.add_option("-c", "--confPath", action="store", type="string", dest="confPath", help="Set cassandra/conf/ path.")
    parser.add_option("-p", "--hconfPath", action="store", type="string", dest="hconfPath", help="Set hadoop/conf/ path.")

    (options, args) = parser.parse_args()
    if options:
        if options.clusterName:
            clusterName = options.clusterName
        if options.seedList:
            seedList = options.seedList
            seedList = seedList.replace(' ', '')
            seedList = seedList.split(',')
            seedList.sort()
        if options.clusterSize:
            clusterSize = options.clusterSize
        if options.autoBootstrap:
            autoBootstrap = options.autoBootstrap
        if options.internalIP:
            internalIP = options.internalIP
        if not options.tokenPosition == None:
            tokenPosition = options.tokenPosition
            if not tokenPosition < clusterSize:
                print "ERROR: Tokens must start at 0 and be less than clusterSize."
                sys.exit()
        if options.confPath:
            confPath = options.confPath
        if options.hconfPath:
            hconfPath = options.hconfPath
        return True
    else:
        return False

def configureCassandraYaml():
    with open(confPath + 'cassandra.yaml', 'r') as f:
        yaml = f.read()

    # Set auto_bootstrap to true if expanding
    if autoBootstrap:
        yaml = yaml.replace('auto_bootstrap: false', 'auto_bootstrap: true')
    else:
        yaml = yaml.replace('auto_bootstrap: true', 'auto_bootstrap: false')

    # Create the seed list
    seedsYaml = ''
    for ip in seedList:
        seedsYaml += '     - ' + ip + '\n'
    if DEBUG:
        print "[DEBUG] seedsYaml: \n" + seedsYaml
    
    # Set seeds
    p = re.compile('seeds:(\s*-.*)*\s*#')
    yaml = p.sub('seeds:\n' + seedsYaml + '\n\n#', yaml)
    
    # Set listen_address
    p = re.compile('listen_address:.*\s*#')
    yaml = p.sub('listen_address: ' + internalIP + '\n\n#', yaml)
    if DEBUG:
        print "[DEBUG] listen_address: " + internalIP
    
    # Set rpc_address
    yaml = yaml.replace('rpc_address: localhost', 'rpc_address: 0.0.0.0')
    
    # Set cluster_name to reservationid
    p = re.compile('cluster_name: .*\s*#')
    yaml = p.sub("cluster_name: '" + clusterName + "'\n\n#", yaml)
    if DEBUG:
        print "[DEBUG] clusterName: " + clusterName
    
    # Construct token for an equally split ring
    print clusterSize
    print tokenPosition
    print (tokenPosition < 0)
    print not (tokenPosition < 0)
    if clusterSize and not (tokenPosition < 0):
        token = tokenPosition * (2**127 / clusterSize)
        p = re.compile( 'initial_token:(\s)*#')
        yaml = p.sub( 'initial_token: ' + str(token) + "\n\n#", yaml)
        if DEBUG:
            print "[DEBUG] clusterSize: " + str(clusterSize)
            print "[DEBUG] tokenPosition: " + str(tokenPosition)
            print "[DEBUG] token: " + str(token)
    
    # Brisk: Set to use different datacenters
    # yaml = yaml.replace('endpoint_snitch: org.apache.cassandra.locator.SimpleSnitch', 'endpoint_snitch: org.apache.cassandra.locator.PropertyFileSnitch')
    
    with open(confPath + 'cassandra.yaml', 'w') as f:
        f.write(yaml)
    
    print '[INFO] cassandra.yaml configured.'
    
def configureMapredSiteXML():
    with open(hconfPath + 'mapred-site.xml', 'r') as f:
        mapredSite = f.read()

    if len(seedList) > 0:
        mapredSite = mapredSite.replace('<value>localhost:8012</value>', '<value>' + seedList[0] + ':8012</value>')
    
    with open(hconfPath + 'mapred-site.xml', 'w') as f:
        f.write(mapredSite)
        
    print '[INFO] mapred-site.xml configured.'
    
def configureOpsCenterConf():
    try:
        with open(opsConfPath + 'opscenterd.conf', 'r') as f:
            opsConf = f.read()
            
        if len(seedList) > 0:
            opsConf = opsConf.replace('interface = 127.0.0.1', 'interface = 0.0.0.0')
            opsConf = opsConf.replace('seed_hosts = localhost', 'seed_hosts = ' + seedList[0])
        
        with open(opsConfPath + 'opscenterd.conf', 'w') as f:
            f.write(opsConf)
            
        print '[INFO] opscenterd.conf configured.'
    except:
        print '[INFO] opscenterd.conf not configured.'




if not commandLineSwitches():
    promptUserInfo()

configureCassandraYaml()
configureMapredSiteXML()
configureOpsCenterConf()
