# TODO - do we need to add to our license something that thanks those repositories?

"""
Lumigo Tracer tries to use as little external libraries as possible, but sometimes it's just not possible.
in these cases (When the library is small enough) we copy the code to our repository and use it from there, in order to
avoid dependency version conflict with the users dependencies.

The source code is not altered in any way, except for:
* Imports between libraries are changed to use the local copy.
"""
