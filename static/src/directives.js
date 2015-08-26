var george = angular.module('george');

george.directive('forceLayoutGraph', ['Flask', ForceLayoutGraph]);
function ForceLayoutGraph() {
    var directive = {
        scope: {},
        restrict: 'AE',
        templateUrl: '../views/graph-directive.html'
    };

    directive.link = function($scope, $element, $attrs, Flask) {
    };

    directive.controller = function($scope, Flask) {
        $scope.messages = [];
        $scope._set = new Set();

        $scope.remove = function(m) {
            if ($scope._set.has(m)) {
                var i = $scope.messages.indexOf(m);
                $scope.messages.splice(i,1);
                $scope._set.delete(m);
                $scope.$apply();
            }
        };

        var w = 900;
        var h = 500;
        var svg = d3.select('#force-layout-graph').append('svg')
                    .attr('width', w)
                    .attr('height', h);

        $scope.render = function() {
            if (!$scope.college) {
                $scope.college = 'Georgia Tech';
            }
            if (!$scope.threshold) {
                $scope.threshold = 0.2;
                
            }
            if (!$scope.startdate) {
                $scope.startdate = 'Jul 6 2015';
            }
            if (!$scope.enddate) {
                $scope.enddate = 'Jul 9 2015';
            }
            Flask.createGraph(
                $scope.college, $scope.startdate, $scope.enddate, $scope.threshold)
                .success(function(response, status) {
                    svg.selectAll("*").remove();
                    var data = response.data;
                    var force = d3.layout.force()
                                    .nodes(data.nodes)
                                    .links(data.edges)
                                    .size([w, h])
                                    .linkDistance([20])
                                    .charge([-20])
                                    .start();

                    var edges = svg.selectAll('line')
                                    .data(data.edges)
                                    .enter()
                                    .append('line')
                                    .style('stroke','#ccc')
                                    .style('stroke-width', 1);

                    var nodes = svg.selectAll('circle')
                                    .data(data.nodes)
                                    .enter()
                                    .append('circle')
                                    .attr('r', 5)
                                    .style('fill', function(d) {return d.color;})
                                    .call(force.drag);

                    nodes.on('click', function(d) {
                        if (!$scope._set.has(d.title)) {
                            $scope.messages.push(d.title);
                            $scope._set.add(d.title);
                        }
                        $scope.$apply();
                    });
                        force.on('tick', function() {
                            edges.attr('x1', function(d) {return d.source.x;});
                            edges.attr('y1', function(d) {return d.source.y;});
                            edges.attr('x2', function(d) {return d.target.x;});
                            edges.attr('y2', function(d) {return d.target.y;});

                            nodes.attr('cx', function(d) {return d.x;});
                            nodes.attr('cy', function(d) {return d.y;});
                        });

                })
                .error(function(response, status) {
                    alert('Server side error. Please see console');
                });
        };

    };
    return directive;
}