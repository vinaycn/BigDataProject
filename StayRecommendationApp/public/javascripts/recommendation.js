/**
 * Created by vinay on 4/24/17.
 */


var myApp = angular.module("Recommendation",["chart.js","rzModule","ui.bootstrap"]);


/*myApp.filter('split', function() {
    return function(input, delimiter) {
        delimiter = delimiter || ','

        return input.split(delimiter);
    }
});*/



/*myApp.controller("RecommendationController",function ($scope,$http){






    $scope.RecommendationPreference = {
        prefferedPlace : false,
        luzury : false,
        commomPlace : false
    };

    $scope.getRecommendation = function(){


        var ws = new WebSocket("ws://localhost:9000/getRecommendation");


        ws.onopen = function(){


            ws.send('111');

        }

        ws.onmessage = function(recommendedListing){


            $scope.RecommendedListings = recommendedListing.data





        }

    };


});*/

function getRecommendation()
{



    // Let us open a web socket
    var ws = new WebSocket("ws://localhost:9000/getRecommendation");

    ws.onopen = function()
    {
        var op1 = $("#exampleSelect1").val()
        var op2 = $("#exampleSelect2").val()
        var op3 = $("#exampleSelect3").val()
        var userPrefernec =  op1+op2+op3;
        ws.send(userPrefernec);
        //alert("Message is sent...");
    };

    ws.onmessage = function (evt)
    {
        var recommendedListings = evt.data;

        var dataArray = jQuery.parseJSON(recommendedListings);

        for(i=0; i<dataArray.length;i++)
        {
            mainDiv  = $("#recommendation");
            mainDiv.append("<div class='row'>");
            mainDiv.append("<div class='col-md-6 portfolio-item'>");
            var first = ('<a href='+dataArray[i][0]+'><img class="img-responsive" src='+dataArray[i][1]+'/></a>')
            mainDiv.append(first);
            var second = ('<h3><a href='+dataArray[i][0]+'>Click Here</a></h3>');
            mainDiv.append(second);
            var third = ('<p>Host Name:<span>'+dataArray[i][2]+'</span></p>');
            var fourth = ('<p>Price : <span>'+dataArray[i][3]+'</span></p>');
            mainDiv.append(third);
            mainDiv.append(fourth);
            mainDiv.append('</div>');
            mainDiv.append('</div>');
        }
    };

    ws.onclose = function()
    {

    };

}

