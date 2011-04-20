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

# Interactively prompts for information to setup the cluster
def promptUserInfo():
    global clusterName, seedList, clusterSize, autoBootstrap, internalIP, tokenPosition
    print "A programmatic interface is also available. Run ./config -h to see all the options.\n"

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
    
    while (tokenPosition < 0) or (tokenPosition >= clusterSize):
        tokenPosition = raw_input("This node's token position (position < cluster size):\n")
        try:
            tokenPosition = int(tokenPosition)
        except:
            print "Please enter a valid number."

# Reads the command line switches when running the program.
def commandLineSwitches():
    global clusterName, seedList, clusterSize, autoBootstrap, internalIP, tokenPosition, confPath, hconfPath, opsConfPath
    parser = OptionParser()
    parser.add_option("-n", "--clusterName", action="store", type="string", dest="clusterName", help="Set the cluster name.")
    parser.add_option("-l", "--seedList", action="store", type="string", dest="seedList", help="Provide a comma-delimited list of seeds.")
    parser.add_option("-s", "--clusterSize", action="store", type="int", dest="clusterSize", help="Provide the size of the cluster for equal partitioning.")
    parser.add_option("-a", "--autoBootstrap", action="store_true", dest="autoBootstrap", help="Set autobootstrap to true. (Defaults to false.)")
    parser.add_option("-i", "--internalIP", action="store", type="string", dest="internalIP", help="Set this nodes internal ip address.")
    parser.add_option("-t", "--tokenPosition", action="store", type="int", dest="tokenPosition", help="Set this node's token position starting at 0.")


    parser.add_option("-c", "--confPath", action="store", type="string", dest="confPath", help="Set cassandra/conf/ path.")
    parser.add_option("-p", "--hconfPath", action="store", type="string", dest="hconfPath", help="Set hadoop/conf/ path.")

    (options, args) = parser.parse_args()
    switchesUsed = False
    if options.clusterName:
        clusterName = options.clusterName
        switchesUsed = True
    if options.seedList:
        seedList = options.seedList
        seedList = seedList.replace(' ', '')
        seedList = seedList.split(',')
        seedList.sort()
        switchesUsed = True
    if options.clusterSize:
        clusterSize = options.clusterSize
        switchesUsed = True
    if options.autoBootstrap:
        autoBootstrap = options.autoBootstrap
        switchesUsed = True
    if options.internalIP:
        internalIP = options.internalIP
        switchesUsed = True
    if not options.tokenPosition == None:
        tokenPosition = options.tokenPosition
        if not tokenPosition < clusterSize:
            print "ERROR: Tokens must start at 0 and be less than clusterSize."
            sys.exit()
        switchesUsed = True
    if options.confPath:
        confPath = options.confPath
        switchesUsed = True
    if options.hconfPath:
        hconfPath = options.hconfPath
        switchesUsed = True
    return switchesUsed

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
    p = re.compile('listen_address:.*')
    yaml = p.sub('listen_address: ' + internalIP, yaml)
    if DEBUG:
        print "[DEBUG] listen_address: " + internalIP
    
    # Set rpc_address
    yaml = yaml.replace('rpc_address: localhost', 'rpc_address: 0.0.0.0')
    
    # Set cluster_name to reservationid
    p = re.compile('cluster_name:.*')
    yaml = p.sub("cluster_name: '" + clusterName + "'", yaml)
    if DEBUG:
        print "[DEBUG] clusterName: " + clusterName
    
    # Construct token for an equally split ring
    if clusterSize and not (tokenPosition < 0):
        token = tokenPosition * (2**127 / clusterSize)
        p = re.compile( 'initial_token:.*')
        yaml = p.sub( 'initial_token: ' + str(token), yaml)
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
    
    # Set job tracker address
    if len(seedList) > 0:
        p = re.compile('<name>mapred.job.tracker</name>\s*.*</value>')
        mapredSite = p.sub('<name>mapred.job.tracker</name>\n  <value>' + seedList[0] + ':8012</value>', mapredSite)
    
    with open(hconfPath + 'mapred-site.xml', 'w') as f:
        f.write(mapredSite)
        
    print '[INFO] mapred-site.xml configured.'
    
def configureOpsCenterConf():
    try:
        with open(opsConfPath + 'opscenterd.conf', 'r') as f:
            opsConf = f.read()
        
        # Set OpsCenter to listen on all addresses and point to a single seed.
        if len(seedList) > 0:
            opsConf = opsConf.replace('interface = 127.0.0.1', 'interface = 0.0.0.0')
            p = re.compile('seed_hosts:.*')
            opsConf = p.sub('seed_hosts = ' + seedList[0], opsConf)
        
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
