'use strict';
var george = angular.module('george', ['ui.router']);

george.run(function($rootScope) {
  $rootScope.$on("$stateChangeError", console.log.bind(console));
});
george.config(function($stateProvider, $urlRouterProvider) {
   
});