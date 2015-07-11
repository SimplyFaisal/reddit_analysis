var george = angular.module('george');
george.service('Flask', Flask);

Flask.$inject = ['$http'];
function Flask($http) {
    var service = {};

    service.createGraph = function(college, day, threshold) {
        return $http({
            method: 'GET',
            url: '/' + college + '/' + day +'/' + threshold,
            data: {
                day: day,
                threshold: threshold
            }
        });
    };

    service.getColleges = function() {
        return $http('/colleges');
    };

    service.getPost = function(postId) {
        return $http('/post/' + postId);
    };

    service.getComment = function(commentId) {
        return $http('/comment/' + commentId);
    };

    service.getComments = function(postId) {
        return $http('/post/comments/' + postId);
    };
    return service;
}
