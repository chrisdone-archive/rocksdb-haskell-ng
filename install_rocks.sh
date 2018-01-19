echo " Installing Rocks DB, g++ compilers & Dependencies "
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test && sudo apt-get -qq update && sudo apt-get -qq install g++-4.8 && sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 90
sudo apt-get install build-essential -y
sudo apt-get install -y libsnappy-dev zlib1g-dev libbz2-dev
cd /tmp
git clone --branch v4.1 --single-branch --depth 1 https://github.com/facebook/rocksdb.git
cd rocksdb
make shared_lib
sudo INSTALL_PATH=/usr/local make install-shared
sudo ldconfig
