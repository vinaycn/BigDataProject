/**
 * Created by vinay on 4/11/17.
 */


angular.module("UserRecommendation",["chart.js","rzModule","ui.bootstrap"]).controller("UserWebSocket",function ($scope,$http) {


    $scope.message = "";
    $scope.response = "";
    var ws  = new WebSocket('ws://localhost:9000/getRecommendation');

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

    ws.onmessage =function(dataFromServer){
        alert("got message")
        alert(dataFromServer.data)
        $scope.response = dataFromServer.data
    };






    $scope.getGraph = function () {
        alert("Getting Graph")
        $http.get('/graph').success(function (stats) {
             alert(stats);
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


    $scope.getGraph1 = function () {
        alert("Getting Graph")
        $http.get('/graph1').success(function (stats) {
            alert(stats);
            console.log(stats);
            //$scope.myData = stats;
            $scope.labels1 =[];
            $scope.data1=[];
            for(var i in stats){

                $scope.labels1.push(i);
                $scope.data1.push(stats[i]);
            }
            /*$scope.labels = ['2006', '2007', '2008', '2009', '2010', '2011', '2012'];
             // $scope.series = ['Series A', 'Series B'];

             $scope.data = [
             [65, 59, 80, 81, 56, 55, 40],
             ];*/

        });

    }




});
