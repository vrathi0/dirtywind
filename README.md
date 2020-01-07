# Dirty Wind

**dirtywind** is a **R** package that uses the _great_ [splitr][1] to model wind
trajectories and join these data with different sources of pollution (i.e. MODIS
AOD and EPA's AQS). This library adds some changes over the *splitr* package.
First, we add PostGIS support, and also add some small parallel loops to
accelerate modeling time.

This is a work in progress: 

- [x] Add repo and split from old Python code
- [x] Add package configuration 
- [x] Add PostGIS connection to functions

[1]: https://github.com/rich-iannone/splitr
