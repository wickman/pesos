apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get -y install \
    curl \
    git \
    libapr1-dev \
    libcurl4-openssl-dev \
    libsasl2-dev \
    libsvn-dev \
    lxc-docker \
    openjdk-7-jdk \
    python-dev \
    python-pip \
    zookeeper

# Set the hostname to the IP address.  This simplifies things for components
# that want to advertise the hostname to the user, or other components.
hostname 192.168.33.2
MESOS_VERSION=0.21.1

# Ensure java 7 is the default java.
update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java

function install_mesos {
  wget --quiet -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_${MESOS_VERSION}-1.0.ubuntu1204_amd64.deb
  dpkg --install mesos_${MESOS_VERSION}-1.0.ubuntu1204_amd64.deb
}

function install_ssh_config {
  cat >> /etc/ssh/ssh_config <<EOF

# Allow local ssh w/out strict host checking
Host $(hostname)
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
EOF
}

function prepare_services {
  # Install the upstart configurations.
  cp /vagrant/vagrant/upstart/*.conf /etc/init
}

function start_services {
  start zookeeper || true
  start mesos-master || true
  start mesos-slave || true
}

function prepare_python {
  pip install wheel
}

install_ssh_config
install_mesos
prepare_services
prepare_python
start_services
