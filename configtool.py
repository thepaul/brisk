#!/usr/bin/env python

import re

clusterlist = ""

def constructYaml():
    with open(confPath + 'cassandra.yaml', 'r') as f:
        yaml = f.read()

    # Set auto_bootstrap to true if expanding
    if options and options.expand:
        yaml = yaml.replace('auto_bootstrap: false', 'auto_bootstrap: true')

    # Create the seed list
    if len(clusterlist) > 0:
        print "[INFO] Using a list of seeds"
        seedsYaml = ''
        for ip in clusterlist:
            seedsYaml += '     - ' + ip + '\n'
        print "[DEBUG] seedsYaml: " + seedsYaml
    else:
        print "[INFO] Using a single seed"
        seedsYaml = '     - ' + clusterseed + '\n'
    
    # Set seeds
    p = re.compile('seeds:(\s*-.*)*\s*#')
    yaml = p.sub('seeds:\n' + seedsYaml + '\n\n#', yaml)
    
    # Set listen_address
    p = re.compile('listen_address:.*\s*#')
    yaml = p.sub('listen_address: ' + internalip + '\n\n#', yaml)
    
    # Set rpc_address
    yaml = yaml.replace('rpc_address: localhost', 'rpc_address: 0.0.0.0')
    
    # Set cluster_name to reservationid
    yaml = yaml.replace("cluster_name: 'Test Cluster'", "cluster_name: '" + reservationid + "'")
    
    # Construct token for an equally split ring
    if options and options.clustersize:
        # Check for user error in generating tokens
        if (launchindex < options.clustersize):
            token = launchindex * (2**127 / int(options.clustersize))
        else:
            # If user error is less than 2x the cluster size add another pass around the ring by splitting the ranges in half
            token = (launchindex % options.clustersize) * (2**127 / int(options.clustersize)) + ((2**127 / int(options.clustersize)) / 2)
        p = re.compile( 'initial_token:(\s)*#')
        yaml = p.sub( 'initial_token: ' + str(token) + "\n\n#", yaml)
    
    # Brisk
    if options and options.clustersize and options.vanillanodes:
        yaml = yaml.replace('endpoint_snitch: org.apache.cassandra.locator.SimpleSnitch', 'endpoint_snitch: org.apache.cassandra.locator.PropertyFileSnitch')
    
    with open(confPath + 'cassandra.yaml', 'w') as f:
        f.write(yaml)
    
    print '[INFO] cassandra.yaml configured.'
    
    with open(hconfPath + 'mapred-site.xml', 'r') as f:
        mapredSite = f.read()

    mapredSite = mapredSite.replace('<value>localhost:8012</value>', '<value>' + clusterseed + ':8012</value>')
    
    with open(hconfPath + 'mapred-site.xml', 'w') as f:
        f.write(mapredSite)
        
    print '[INFO] mapred-site.xml configured.'
    
    with open(opsConfPath + 'opscenterd.conf', 'r') as f:
        opsConf = f.read()
        
    opsConf = opsConf.replace('interface = 127.0.0.1', 'interface = 0.0.0.0')
    opsConf = opsConf.replace('seed_hosts = localhost', 'seed_hosts = ' + clusterseed)
    
    with open(opsConfPath + 'opscenterd.conf', 'w') as f:
        f.write(opsConf)
        
    print '[INFO] opscenterd.conf configured.'

