# pollution_wind:

**pollution_wind** is a **R** package that uses the _great_ [splitr][1] to model
wind trajectories and join these data with different sources of pollution (i.e.
MODIS AOD and EPA's AQS). This library adds some changes over the *splitr*
package. First, we add PostGIS support, and also add some small parallel loops
to accelerate modeling time.

This is a work in progress: 

- [] Add repo and split from old Python code
- [] Add package configuration 
- [] Add PostGIS connection to functions
- [] Fork **splitr** and add `foreach` and other possible parallel functions

[1]: https://github.com/rich-iannone/splitr
