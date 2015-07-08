'use strict';
var george = angular.module('george', ['ui.router']);
george.config(function($stateProvider, $urlRouterProvider) {
   $urlRouterProvider.otherwise('main'); 
   $stateProvider.state('main',{
        url:'main',
        templateUrl: '../../views/main.html'   
    });
});