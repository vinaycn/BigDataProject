/**
 * Created by vinay on 4/24/17.
 */
angular.module("AnalysisData",[]).controller("AnalysisDataController",function ($scope,$http) {


    $scope.getFullData = function(){

        $http.get('/loadData').success(function (tableData) {

            alert("Got data");
            console.log(tableData);
            $scope.noumberOfRows = tableData;


        });

    };





});