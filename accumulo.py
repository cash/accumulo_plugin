import posixpath

from starcluster import threadpool
from starcluster import clustersetup
from starcluster.logger import log

zoo_cfg_templ = """\
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181
maxClientCnxns=100
"""

accumulo_env_templ = """\
test -z "$JAVA_HOME"             && export JAVA_HOME=/usr/lib/jvm/java-6-sun/jre
test -z "$HADOOP_HOME"           && export HADOOP_HOME=/opt/hadoop
test -z "$ZOOKEEPER_HOME"        && export ZOOKEEPER_HOME=/opt/zookeeper
test -z "$ACCUMULO_LOG_DIR"      && export ACCUMULO_LOG_DIR=$ACCUMULO_HOME/logs
if [ -f ${ACCUMULO_HOME}/conf/accumulo.policy ]
then
   POLICY="-Djava.security.manager -Djava.security.policy=${ACCUMULO_HOME}/conf/accumulo.policy"
fi
test -z "$ACCUMULO_TSERVER_OPTS" && export ACCUMULO_TSERVER_OPTS="${POLICY} -Xmx384m -Xms384m "
test -z "$ACCUMULO_MASTER_OPTS"  && export ACCUMULO_MASTER_OPTS="${POLICY} -Xmx128m -Xms128m"
test -z "$ACCUMULO_MONITOR_OPTS" && export ACCUMULO_MONITOR_OPTS="${POLICY} -Xmx64m -Xms64m"
test -z "$ACCUMULO_GC_OPTS"      && export ACCUMULO_GC_OPTS="-Xmx64m -Xms64m"
test -z "$ACCUMULO_LOGGER_OPTS"  && export ACCUMULO_LOGGER_OPTS="-Xmx384m -Xms256m"
test -z "$ACCUMULO_GENERAL_OPTS" && export ACCUMULO_GENERAL_OPTS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75"
test -z "$ACCUMULO_OTHER_OPTS"   && export ACCUMULO_OTHER_OPTS="-Xmx128m -Xms64m"
export ACCUMULO_LOG_HOST=`(grep -v '^#' $ACCUMULO_HOME/conf/masters ; echo localhost ) 2>/dev/null | head -1`
"""

accumulo_site_templ = """\
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
	<!--  Put your site-specific accumulo configurations here. -->

    <property>
      <name>instance.zookeeper.host</name>
      <value>%(master)s:2181</value>
      <description>comma separated list of zookeeper servers</description>
    </property>

    <property>
      <name>logger.dir.walog</name>
      <value>/mnt/walogs</value>
      <description>The directory used to store write-ahead logs on the local filesystem. It is possible to specify a comma-separated list of directories.</description>
    </property>
    
    <property>
      <name>instance.secret</name>
      <value>todo_convert_to_plugin_option</value>
      <description>A secret unique to a given instance that all servers must know in order to communicate with one another.</description>
    </property>

    <property>
      <name>tserver.memory.maps.max</name>
      <value>1G</value>
    </property>
    
    <property>
      <name>tserver.cache.data.size</name>
      <value>7M</value>
    </property>
    
    <property>
      <name>tserver.cache.index.size</name>
      <value>20M</value>
    </property>
    
    <property>
      <name>trace.password</name>
      <!-- 
        change this to the root user's password, and/or change the user below 
       -->
      <value>secret</value>
    </property>
    
    <property>
      <name>trace.user</name>
      <value>root</value>
    </property>
    
    <property>
      <name>logger.sort.buffer.size</name>
      <value>50M</value>
    </property>

    <property>
      <name>tserver.walog.max.size</name>
      <value>100M</value>
    </property>

    <property>
      <name>general.classpaths</name>
      <value>
    $ACCUMULO_HOME/src/server/target/classes/,
    $ACCUMULO_HOME/src/core/target/classes/,
    $ACCUMULO_HOME/src/start/target/classes/,
    $ACCUMULO_HOME/src/examples/target/classes/,
	$ACCUMULO_HOME/lib/[^.].$ACCUMULO_VERSION.jar,
	$ACCUMULO_HOME/lib/[^.].*.jar,
	$ZOOKEEPER_HOME/zookeeper[^.].*.jar,
	$HADOOP_HOME/conf,
	$HADOOP_HOME/[^.].*.jar,
	$HADOOP_HOME/lib/[^.].*.jar,
      </value>
      <description>Classpaths that accumulo checks for updates and class files.
      When using the Security Manager, please remove the ".../target/classes/" values.
      </description>
    </property>

</configuration>
"""

class AccumuloInstaller(clustersetup.ClusterSetup):
    """
    Configures Accumulo
    """

    def __init__(self, hadoop_tmpdir='/mnt/hadoop'):
        self.hadoop_tmpdir = hadoop_tmpdir
        self.hadoop_home = '/opt/hadoop'
        self.accumulo_home = '/opt/accumulo'
        self.accumulo_conf = '/opt/accumulo/conf'
        self._pool = None

    @property
    def pool(self):
        if self._pool is None:
            self._pool = threadpool.get_thread_pool(20, disable_threads=False)
        return self._pool

    def _install_zookeeper(self, master):
        log.info("Configuring Zookeeper...")
        commands = [
            #"wget http://apache.osuosl.org/zookeeper/zookeeper-3.3.6/zookeeper-3.3.6.tar.gz",
            #"tar xzf zookeeper-3.3.6.tar.gz",
            #"mv zookeeper-3.3.6 /opt/",
            #"ln -s /opt/zookeeper-3.3.6/ /opt/zookeeper",
            "mkdir /var/lib/zookeeper",
        ]
        for cmd in commands:
            master.ssh.execute(cmd)
        zoo_cfg = master.ssh.remote_file('/opt/zookeeper/conf/zoo.cfg')
        zoo_cfg.write(zoo_cfg_templ)
        zoo_cfg.close()

    def _start_zookeeper(self, master):
        log.info("Starting zookeeper...")
        master.ssh.execute('/opt/zookeeper/bin/zkServer.sh start')

    def _download_accumulo(self, node):
        commands = [
            "wget http://apache.osuosl.org/accumulo/1.4.3/accumulo-1.4.3-dist.tar.gz",
            "tar xzf accumulo-1.4.3-dist.tar.gz",
            "mv accumulo-1.4.3 /opt/",
            "ln -s /opt/accumulo-1.4.3/ /opt/accumulo",
        ]
        for cmd in commands:
            node.ssh.execute(cmd)

    def _configure_env(self, node, cfg):
        env_sh = posixpath.join(self.accumulo_conf, 'accumulo-env.sh')
        env = node.ssh.remote_file(env_sh)
        env.write(accumulo_env_templ % cfg)
        env.close()

    def _configure_site(self, node, cfg):
        site_xml = posixpath.join(self.accumulo_conf, 'accumulo-site.xml')
        xml = node.ssh.remote_file(site_xml)
        xml.write(accumulo_site_templ % cfg)
        xml.close()

    def _configure_slaves(self, node, slaves):
        slaves_file = posixpath.join(self.accumulo_conf, 'slaves')
        slaves_file = node.ssh.remote_file(slaves_file)
        slaves_file.write('\n'.join(slaves))
        slaves_file.close()

    def _configure_masters(self, node, master):
        masters_file = posixpath.join(self.accumulo_conf, 'masters')
        masters_file = node.ssh.remote_file(masters_file)
        masters_file.write(master.private_ip_address)
        masters_file.close()

    def _copy_files(self, node):
        node.ssh.execute('%s/bin/config.sh' % self.accumulo_home)
        node.ssh.execute('cp /opt/accumulo/conf/examples/1GB/standalone/accumulo-metrics.xml /opt/accumulo/conf/')
        node.ssh.execute('cp /opt/accumulo/conf/examples/1GB/standalone/log4j.properties /opt/accumulo/conf/')
        node.ssh.execute('cp /opt/accumulo/conf/examples/1GB/standalone/generic_logger.xml /opt/accumulo/conf/')
        node.ssh.execute('cp /opt/accumulo/conf/examples/1GB/standalone/monitor_logger.xml /opt/accumulo/conf/')

    def _configure_accumulo(self, master, nodes):
        cfg = {'master': master.private_ip_address}

        log.info("Configuring Accumulo Env...")
        for node in nodes:
            self.pool.simple_job(self._configure_env, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring Accumulo Site...")
        for node in nodes:
            self.pool.simple_job(self._configure_site, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring masters file...")
        for node in nodes:
            self.pool.simple_job(self._configure_masters, (node, master),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring slaves file...")
        slaves = [x.private_ip_address for x in nodes if x.private_ip_address != master.private_ip_address]
        for node in nodes:
            self.pool.simple_job(self._configure_slaves, (node, slaves),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Additional configuration (logging and monitoring)...")
        for node in nodes:
            self.pool.simple_job(self._copy_files, (node),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

    def _start_accumulo(self, master, nodes):
        log.info("Starting accumulo...")
        master.ssh.execute('%s/bin/start-all.sh' % self.accumulo_home)


    def _open_ports(self, master):
        ports = [50095]
        ec2 = master.ec2
        for group in master.cluster_groups:
            for port in ports:
                has_perm = ec2.has_permission(group, 'tcp', port, port,
                                              '0.0.0.0/0')
                if not has_perm:
                    group.authorize('tcp', port, port, '0.0.0.0/0')

    def run(self, nodes, master, user, user_shell, volumes):
        self._install_zookeeper(master)
        self._start_zookeeper(master)

        self._configure_accumulo(master, nodes)
        #self._start_accumulo(master, nodes)

        self._open_ports(master)
        log.info("Accumulo monitor page: http://%s:50095" % master.dns_name)

