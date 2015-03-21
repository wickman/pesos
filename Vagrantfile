# Vagrantfile API/syntax version. Don't touch unless you know what you're
# doing!
VAGRANTFILE_API_VERSION = "2"

# 1.5.0 is required to use vagrant cloud images.
# https://www.vagrantup.com/blog/vagrant-1-5-and-vagrant-cloud.html
Vagrant.require_version ">= 1.5.0"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.define "testcluster" do |dev|
    dev.vm.network :private_network, ip: "192.168.33.2"
    # dev.vm.network "forwarded_port", guest: 31337, host: 41337
    dev.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "1536"]
    end
    dev.vm.provision "shell", path:
    "vagrant/provision.sh"
  end
end
