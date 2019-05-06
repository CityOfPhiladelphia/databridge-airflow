# Update your machine
sudo apt-get update -yqq

# Install Docker and git
sudo apt-get install -yqq --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common \
&& curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add - \
&& sudo add-apt-repository \
    "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) \
    stable" \
&& sudo apt-get install -yqq --no-install-recommends \
    git-core \
    docker-ce \
    docker-ce-cli \
    containerd.io

# Install docker-compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && sudo chmod +x /usr/local/bin/docker-compose

# Install AWS Code Deploy
sudo apt-get install ruby
wget https://aws-codedeploy-us-east-1.s3.amazonaws.com/latest/install
chmod +x ./install
sudo ./install auto
rm ./install

# Check the status
sudo service codedeploy-agent status

# If you see a message like error: No AWS CodeDeploy agent running, manually start the agent
# sudo service codedeploy-agent start

# Clone this repo and enter its directory
git clone https://github.com/CityOfPhiladelphia/databridge-airflow
cd databridge-airflow