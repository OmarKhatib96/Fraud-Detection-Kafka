<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Thanks again! Now go create something AMAZING! :D
-->



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/othneildrew/Best-README-Template">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Best-README-Template</h3>


<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

[![Product Name Screen Shot][product-screenshot]](https://example.com)
The goal of the project is to create a Flink application which will read from Kafka clicks and displays, detect some suspicious/fraudulent activities and output the suspicious events into a file.
Please download and run this docker-compose file : https://github.com/Sabmit/paris-dauphine/blob/master/docker/kafka-zk/docker-compose.yml
To run it, simply execute `docker-compose rm -f; docker-compose up` in the same directory as the docker-compose.yml file.
By running this docker-compose, it will :
Create a Kafka cluster with two topics : "clicks" and "displays"
Launch a python script which will send events to those two topics and display them in the terminal
This generator simulates few suspicious/fraudulent patterns that you should detect using Flink.
There are 3 distincts patterns we're trying  to find.


### Built With

This section should list any major frameworks that you built your project using. Leave any add-ons/plugins for the acknowledgements section. Here are a few examples.
* [Docker](https://docs.docker.com/)
* [Maven](https://maven.apache.org/)
* [IntelliJ](https://flink.apache.org/)



<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

This is an example of how to list things you need to use the software and how to install them.
* npm
  ```sh
  docker-compose up
  ```

### Installation

1. Install Docker Desktop
2. Clone the repo
   ```sh
   git clone https://github.com/OmarKhatib96/Fraud-Detection-Kafka.git
   ```
3. Install Scala and Java environements as well as Maven
   ```sh
   sudo apt install default-jre
   sudo apt-get install scala   
   ```
4. Install IntelliJ or any equivalent IDE like Ellipse (https://www.jetbrains.com/idea/)_
 

<!-- USAGE EXAMPLES -->
## Usage


1. Clone the repo
   ```sh
   git clone https://github.com/OmarKhatib96/Fraud-Detection-Kafka.git
   ```
2. Go to the root folder of the compose file and run
   ```sh
   docker-compose up
   ```
4. Open the quistart folder with IntelliJ and run the project
5. The results of the automated fraud detection are logged into 3 text files
6. Run the notebook on Jupyter environement to get the data visualization.
 
<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

*Omar KHATIB  - omar.khatib@dauphine.eu
*Yanis AMIROU - yanis.amirou@dauphine.eu
Project Link: [https://github.com/OmarKhatib96/Fraud-Detection-Kafka](https://github.com/OmarKhatib96/Fraud-Detection-Kafka)



<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements
* [Flink website](https://flink.apache.org/)
* [IntelliJ](https://www.jetbrains.com/idea/)


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/othneildrew/Best-README-Template.svg?style=for-the-badge
[contributors-url]: https://github.com/othneildrew/Best-README-Template/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/othneildrew/Best-README-Template.svg?style=for-the-badge
[forks-url]: https://github.com/othneildrew/Best-README-Template/network/members
[stars-shield]: https://img.shields.io/github/stars/othneildrew/Best-README-Template.svg?style=for-the-badge
[stars-url]: https://github.com/othneildrew/Best-README-Template/stargazers
[issues-shield]: https://img.shields.io/github/issues/othneildrew/Best-README-Template.svg?style=for-the-badge
[issues-url]: https://github.com/othneildrew/Best-README-Template/issues
[license-shield]: https://img.shields.io/github/license/othneildrew/Best-README-Template.svg?style=for-the-badge
[license-url]: https://github.com/othneildrew/Best-README-Template/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]:https://www.linkedin.com/in/omar-khatib-b0758b12b/
[product-screenshot]: images/screenshot.png
