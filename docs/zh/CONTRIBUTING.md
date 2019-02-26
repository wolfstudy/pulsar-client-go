# 如何参与 native pulsar go client 的开发

## 介绍

`native pulsar go client` 是基于 comcast 的 [pulsar-client-go](https://github.com/Comcast/pulsar-client-go) 来继续 catch up 相应的功能，目前项目被 fork 到 wolfstudy 的 [pulsar-client-go](https://github.com/wolfstudy/pulsar-client-go) 下面，后续的研发工作将在该项目下面进行。

## 前期准备：

- ### 加入 pulsar 的 slack

```
  Pulsar Slack频道: 
  https://apache-pulsar.slack.com/
  可自行在这里注册：
  https://apache-pulsar.herokuapp.com/
```

添加到 `native-go-china`的 channel 中，用于平时开发的交流和协作。

- ### fork 

将  [pulsar-client-go](https://github.com/wolfstudy/pulsar-client-go) fork 到自己的 github 下

## Requirements

[Go](http://golang.org/) 1.11 or newer.

## Installation

If you don't currently have a go environment installed，install Go according to the installation instructions here: [http://golang.org/doc/install](http://golang.org/doc/install)

### mac os && linux

```
$ mkdir -p $HOME/github.com/Comcast
$ cd $HOME/github.com/Comcast
$ git clone git@github.com:wolfstudy/pulsar-client-go.git
$ cd pulsar-client-go
$ go mod tidy
```

### pr 的提交流程

```
$ git remote add apache git@github.com:wolfstudy/pulsar-client-go.git

// sync with remote master
$ git checkout master
$ git fetch apache
$ git rebase apache/master
$ git push origin master

// create PR branch
$ git checkout -b your_branch   
do your work
$ git add x'x'x
$ git commit -sm "xxx"
$ git push origin your_branch
```

### 关于新功能

1. 新功能的开发建议先以issue的形式提交，后面如果有人对这个功能感兴趣，可以直接认领这个issue，然后以pull request的形式提交。
2. 如果在开发工程中需要创建新的文件，请使用下面的lisence header：

```
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
```
