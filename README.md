CMS Recommendation Template
===========================


###Overview

This engine template is based on [SCALA e-commerce recommendion template](http://templates.prediction.io/PredictionIO/template-scala-parallel-ecommercerecommendation) provides personalized recommendation for CMS applications with the following features by default:

Recommend unseen articles only (configurable)
Recommend popular articles if no information about the user is available

###Usage

#Event Data Requirements
By default, this template takes the following data from Event Server:

* Users' view events
* Users' like events
* Users' share events
* Users' rate events
* Articles' with categories and tag properties


Sample training data is not included. However a training data is available for the Mosaic&trade; sandbox. for more information about the platform visit [movent.com](http://www.movent.com)

###Input Query
* UserID
* Num of articles to be recommended
* List of white-listed article categories and/or tags (optional)

The template also supports black-list and whitelist. If a whitelist is provided, the engine will include only those articles in the recommendation. Likewise, if a blacklist is provided, the engine will exclude those articles in the recommendation.

###Output PredictedResult
A ranked list of recommended articleIds

###Install and Run PredictionIO

Follow the instructions [here](https://docs.prediction.io/templates/ecommercerecommendation/quickstart/)

###Create a new Engine from an Engine Template

Follow the instrunction from ecommercerecommendation but replace the engine source to 'rmzamora/template-scala-parallel-cmsrecommendation'


