/**
 * Created by vinay on 4/11/17.
 */


angular.module("UserRecommendation",["chart.js"]).controller("UserWebSocket",function ($scope,$http) {

    var ws = new WebSocket('ws://localhost:9000/socket');
    alert("Hi");

    var chat = this;
     chat.currentMessage = "";

     chat.messages =[];

    $scope.sendMessage = function(){
        alert("Send The Data");
        alert(chat.currentMessage)
        ws.send("Add");
    };




    $scope.getMessage = function () {
        alert("Get The Data");
        ws.send("getFromConsumer");
    }

    ws.onmessage = function(msg){
        alert(msg.data);
        $scope.$digest();
    }




    $scope.getGraph = function () {
        alert("Getting Graph")
        $http.get('/graph').success(function (stats) {
            // alert(data);
            console.log(stats);
            //$scope.myData = stats;
            $scope.labels =[];
            $scope.data=[];
            for(var i in stats){

                $scope.labels.push(i);
                $scope.data.push(stats[i]);
            }
            /*$scope.labels = ['2006', '2007', '2008', '2009', '2010', '2011', '2012'];
           // $scope.series = ['Series A', 'Series B'];

            $scope.data = [
                [65, 59, 80, 81, 56, 55, 40],
            ];*/

        });
        
    }




});
