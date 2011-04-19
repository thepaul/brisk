#!/usr/bin/env python

import re

confPath = ''
hconfPath = ''
opsConfPath = ''

clusterName = ''
clusterList = ''
clusterSize = False
autoBootstrap = False
internalIP = ''
tokenPosition = -1

DEBUG = False

def promptUserInfo():
    global clusterName, clusterList, clusterSize, autoBootstrap, internalIP, tokenPosition
    clusterName = raw_input("Cluster name:\n")
    
    clusterList = raw_input("Cluster list (comma-delimited):\n")
    clusterList = clusterList.replace(' ', '')
    clusterList = clusterList.split(',')

    while (not type(clusterSize) is int):
        clusterSize = raw_input("Current cluster size:\n")
        try:
            clusterSize = int(clusterSize)
        except:
            print "Please enter a valid number."

    while not (autoBootstrap == "y" or autoBootstrap == 'n'):
        autoBootstrap = raw_input("Is this node being autoBootStrapped? [y/n]\n").strip()
    
    internalIP = raw_input("This node's internal IP address:\n")
    
    while (tokenPosition < 0) or (tokenPosition >= clusterSize):
        tokenPosition = raw_input("This node's token position (position < cluster size):\n")
        try:
            tokenPosition = int(tokenPosition)
        except:
            print "Please enter a valid number."

def configureCassandraYaml():
    with open(confPath + 'cassandra.yaml', 'r') as f:
        yaml = f.read()

    # Set auto_bootstrap to true if expanding
    if autoBootstrap:
        yaml = yaml.replace('auto_bootstrap: false', 'auto_bootstrap: true')

    # Create the seed list
    seedsYaml = ''
    for ip in clusterList:
        seedsYaml += '     - ' + ip + '\n'
    if DEBUG:
        print "[DEBUG] seedsYaml: " + seedsYaml
    
    # Set seeds
    p = re.compile('seeds:(\s*-.*)*\s*#')
    yaml = p.sub('seeds:\n' + seedsYaml + '\n\n#', yaml)
    
    # Set listen_address
    p = re.compile('listen_address:.*\s*#')
    yaml = p.sub('listen_address: ' + internalIP + '\n\n#', yaml)
    
    # Set rpc_address
    yaml = yaml.replace('rpc_address: localhost', 'rpc_address: 0.0.0.0')
    
    # Set cluster_name to reservationid
    yaml = yaml.replace("cluster_name: 'Test Cluster'", "cluster_name: '" + clusterName + "'")
    
    # Construct token for an equally split ring
    if clusterSize:
        token = tokenPosition * (2**127 / int(clusterSize))
        p = re.compile( 'initial_token:(\s)*#')
        yaml = p.sub( 'initial_token: ' + str(token) + "\n\n#", yaml)
    
    # Brisk: Set to use different datacenters
    if options and options.clusterSize and options.vanillanodes:
        yaml = yaml.replace('endpoint_snitch: org.apache.cassandra.locator.SimpleSnitch', 'endpoint_snitch: org.apache.cassandra.locator.PropertyFileSnitch')
    
    with open(confPath + 'cassandra.yaml', 'w') as f:
        f.write(yaml)
    
    print '[INFO] cassandra.yaml configured.'
    
def configureMapredSiteXML():
    with open(hconfPath + 'mapred-site.xml', 'r') as f:
        mapredSite = f.read()

    mapredSite = mapredSite.replace('<value>localhost:8012</value>', '<value>' + clusterList[0] + ':8012</value>')
    
    with open(hconfPath + 'mapred-site.xml', 'w') as f:
        f.write(mapredSite)
        
    print '[INFO] mapred-site.xml configured.'
    
def configureOpsCenterConf():
    with open(opsConfPath + 'opscenterd.conf', 'r') as f:
        opsConf = f.read()
        
    opsConf = opsConf.replace('interface = 127.0.0.1', 'interface = 0.0.0.0')
    opsConf = opsConf.replace('seed_hosts = localhost', 'seed_hosts = ' + clusterList[0])
    
    with open(opsConfPath + 'opscenterd.conf', 'w') as f:
        f.write(opsConf)
        
    print '[INFO] opscenterd.conf configured.'




promptUserInfo()
