# Hyrise Example Plugin

This repository contains an example plugin for [Hyrise](https://github.com/hyrise/hyrise "Hyrise on GitHub"). It can be used as a basis for the development of further plugins.
The plugin is introduced in our paper "Hyrise Re-engineered: An Extensible Database System for Research in Relational In-Memory Data Management" (EDBT 2019, Dreseler et al.).


## Development

1. Check out the submodules and their dependencies: `git submodule update --init --recursive`
1. Take a look at the example_plugin files. These can be used as a blueprint for other plugins.
1. Implement your plugin according to the example plugin. Make sure it inherits from `AbstractPlugin` and `Singleton` (hyrise/src/lib/utils/(abstract_plugin || singleton).hpp).
1. Add the necessary information to build your plugin to CMakeLists.txt. A self-explanatory skeleton is provided in the file itself.
1. We highly recommend out-of-source builds, e.g.:
    - `mkdir cmake-build-debug && cd cmake-build-debug`
    - `cmake ..`
    - `make ExamplePlugin -j 12`
1. Test your plugin, for example, using the Hyrise Console (from the main directory):
    - `./cmake-build-debug/hyriseConsole`
    - `load_plugin cmake-build-debug/libExamplePlugin.so` (`libExamplePlugin.dylib` for Mac systems)
