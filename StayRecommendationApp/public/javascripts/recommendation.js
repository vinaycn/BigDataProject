/**
 * Created by vinay on 4/24/17.
 */


var myApp = angular.module("Recommendation",["chart.js","rzModule","ui.bootstrap"]);




myApp.controller("RecommendationController",function ($scope,$http){




    $scope.RecommendationPreference = {
        prefferedPlace : false,
        luzury : false,
        commomPlace : false
    };

    $scope.getRecommendation = function(){


        var ws = new WebSocket("ws://localhost:9000/getRecommendation");


        ws.onopen = function(){

            alert("Opened Ws");
            ws.send('111');

        }

        ws.onmessage = function(data){
            alert("Got Message");
            alert(data)
            $scope.Recommendation = data;


        }

        ws.onclose = function(){
            alert("Closing Ws");

        }

        alert($scope.RecommendationPreference.prefferedPlace);
        alert($scope.RecommendationPreference.luzury);
        alert($scope.RecommendationPreference.commomPlace);
    };


});

