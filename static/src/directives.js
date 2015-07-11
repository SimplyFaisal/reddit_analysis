var george = angular.module('george');

george.directive('forceLayoutGraph', ['Flask', ForceLayoutGraph]);
function ForceLayoutGraph() {
    var directive = {
        scope: {},
        restrict: 'AEC',
        replace: true,
        templateUrl: '../views/graph-directive.html'
    };

    directive.link = function($scope, $element, $attrs) {

    };

    directive.controller = function($scope, Flask) {

        $scope.render = function() {
            if (!($scope.college && $scope.threshold && $scope.date)) {
            }
            Flask.createGraph($scope.college, $scope.date, $scope.threshold)
                .success(function(response, status) {
                    console.log(response.data);
                })
                .error(function(response, status) {
                    alert('Server side error. Please see console');
                });
        };

    };
    return directive;
}