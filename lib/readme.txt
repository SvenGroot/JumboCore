Jumbo uses a custom version Lokad.ILPack, built from https://github.com/SvenGroot/ILPack

This version contains mitigations for https://github.com/Lokad/ILPack/issues/107 (fixed in the
official repository but not yet available in NuGet), and https://github.com/Lokad/ILPack/issues/112
(not yet fixed in the official repository).

This library is not checked in, and must be restored by cloning ILPack from the above source,
and running the following command from the "src" directory:

dotnet publish -c Release -f netcoreapp21 -o <JumboCoreRoot>/lib/ilpack
