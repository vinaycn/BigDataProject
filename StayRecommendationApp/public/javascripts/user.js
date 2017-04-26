/**
 * Created by vinay on 4/11/17.
 */


var myApp = angular.module("MapReduceAnalysis",["chart.js","rzModule","ui.bootstrap"]);

myApp.directive('scrolly', function () {
    return {
        restrict: 'A',
        link: function (scope, element, attrs) {
            var raw = element[0];
            console.log('loading directive');

            element.bind('scroll', function () {
                console.log('in scroll');
                console.log(raw.scrollTop + raw.offsetHeight);
                console.log(raw.scrollHeight);
                if (raw.scrollTop + raw.offsetHeight > raw.scrollHeight) {
                    console.log("I am at the bottom");
                    scope.$apply(attrs.scrolly);
                }
            });
        }
    };
});

    myApp.controller("MainAnalysisController",function ($scope,$http) {



    $scope.city = "Newyork";


    $scope.getInitialAnalysis =function(city){
        $scope.getAnalysisForStayType(city);
        $scope.getAnalysisByNoOfRooms(city);
        $scope.getNoOfListingsByReviewScoreRange(city);
        $scope.getSentimentScoreForThePlace(city);

    }

    $scope.getAnalysisForStayType = function (city) {

        var data = {
            city : city
        };


        $http({
            url : '/getAnaysisForStayType',
            method : 'POST',
            data : data
        }).then(function(stats) {

            var dataStay =stats.data;
            $scope.labels =[];
            $scope.data=[];
            for(var i in dataStay){

                $scope.labels.push(i);
                $scope.data.push(dataStay[i]);
            }
        }, function(failure) {
            alert("Unable to get Data Please Try again Later");
        });
        
    }


        $scope.getAnalysisByNoOfRooms = function (city) {

            var data = {
                city : city
            };


            $http({
                url : '/getAnalysisByNoOfRooms',
                method : 'POST',
                data : data
            }).then(function(stats) {

                var dataRooms = stats.data
                $scope.labels1 =[];
                $scope.data1=[];
                for(var i in dataRooms){
                    $scope.labels1.push(i);
                    $scope.data1.push(dataRooms[i]);
                }
            }, function(failure) {
                alert("Something Went Wrong Please Try again Later");
            });

        }

        $scope.getNoOfListingsByReviewScoreRange = function (city) {

            var data = {
                city : city
            };


            $http({
                url : '/getNoOfListingsByReviewScoreRange',
                method : 'POST',
                data : data
            }).then(function(stats) {

                var dataNumListings = stats.data

                console.log(dataNumListings)
                $scope.labels2 =[];
                $scope.data2=[];
                for(var i in dataNumListings){
                    $scope.labels2.push(i);
                    $scope.data2.push(dataNumListings[i]);
                }
            }, function(failure) {
                alert("Something Went Wrong Please Try again Later");
            });

        }


        $scope.getTop10Litings = function(city) {
            var data = {
                city: city
            };
            $http({
                url: '/getTop10Listings',
                method: 'POST',
                data: data
            }).then(function (stats) {
                $scope.top10 = stats.data;
                console.log(stats);

            }, function (failure) {
                alert("Something Went Wrong Please Try again Later");
            });
        }

            $scope.getSentimentScoreForThePlace = function(city){
                var data = {
                    city : city
                };

                $http({
                    url : '/getSentimentScoreForThePlace',
                    method : 'POST',
                    data : data
                }).then(function(stats) {
                    var sentiment = stats.data
                    $scope.labels3 =[];
                    $scope.data3=[];
                    for(var i in sentiment){
                        $scope.labels3.push(i);
                        $scope.data3.push(sentiment[i]);
                    }

                }, function(failure) {
                    alert("Something Went Wrong Please Try again Later");
                });



    }





});
