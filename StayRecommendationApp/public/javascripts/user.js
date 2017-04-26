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


    $scope.message = "";
    $scope.response = "";


    $scope.minSlider = {
        value: 10
    };
    $scope.minSlider1 = {
        value: 10
    };
    $scope.minSlider2 = {
        value: 10
    };


    $scope.getRecommendation = function(message){

        var one =  $scope.minSlider.value;
        var two = $scope.minSlider1.value;
        var three =$scope.minSlider2.value;

        //ws  = new WebSocket('ws://localhost:9000/getRecommendation');
        alert("Web Socket Connection  established");
        alert(message);
        ws.send(message);
    };





    $scope.city = "Newyork";


    $scope.getInitialAnalysis =function(city){
        $scope.getAnalysisForStayType(city);
        $scope.getAnalysisByNoOfRooms(city);

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
                alert(dataNumListings)
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


        $scope.getTop10Litings = function(city){
        alert("Getting Top 10")

        $http.get('/getTop10Listings').success(function (stats) {
            alert(stats);
            $scope.top10 = stats;
            console.log(stats);

        });
    }





});
