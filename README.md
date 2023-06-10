# Medley and txMontage

Based on the new NonBlock Transaction Composition (NBTC) methodology,
Medley and txMontage are the two general systems for building
transactional nonblocking data
structures for both transient memory and persistent memory,
respectively. The methodology and the systems are introduced by W.
Cai, H. Wen, and M. L. Scott from the University of Rochester. The paper appears at SPAA' 23
([link](https://www.cs.rochester.edu/u/wcai6/papers/2023_Cai_SPAA_NBTC.pdf)).

txMontage is based on
[nbMontage](https://github.com/urcs-sync/Montage), a fully nonblocking
variant of Montage that implements periodic persistence, executing all
cache line writes-back from the same _epoch_ in one batch, therefore
amortizing the write-back overhead to minimum. It also inherits
_wait-free_ sync from nbMontage. You may think txMontage as Medley +
nbMontage.

Due to time constraints during development, Medley and txMontage are
not well separated and refactored --- they are using the same Montage
framework. We call the data structures that allocate in DRAM and
don't use Montage persistence annotation Medley structures, and those
that allocate in NVM and use Montage persistence annotation txMontage
structures. Refactoring is WIP: in the ideal case, they should have
separate base class, `Composable` for Medley structures and
`Recoverable` for txMontage structures.

The testing harness is based on J. Izraelevitz's
[parHarness](https://github.com/izrajoe/parHarness). The persistent
allocator is [Ralloc](https://github.com/urcs-sync/ralloc) from W. Cai et al.

- [Medley and txMontage](#medley-and-txmontage)
  - [1. Required Libraries](#1-required-libraries)
  - [2. Commands](#2-commands)
    - [2.1. Run Specific Test](#21-run-specific-test)
    - [2.3. Use Medley and txMontage in Your Code](#23-use-medley-and-txmontage-in-your-code)
    - [2.4. Different Mount Point](#24-different-mount-point)
  - [3. Static and Dynamic Environment Variables](#3-static-and-dynamic-environment-variables)
    - [3.1. Static Variables](#31-static-variables)
    - [3.2. Dynamic Variables](#32-dynamic-variables)
    - [3.3. Obsolete Variables](#33-obsolete-variables)

## 1. Required Libraries

Source code of most of required libraries are provided in `./ext`,
which includes
[Ralloc](https://github.com/urcs-sync/ralloc),
LFTT ([integrated version](https://github.com/roghnin/tlds) by H. Wen;
originally forked from [here](https://github.com/ucf-cs/tlds)), and
TDSL ([integrated
version](https://github.com/roghnin/TDSL/tree/sl_as_map) by H. Wen;
originally forked from [here](https://github.com/amitz25/TDSL)).

Other than those in `./ext`, this repository also depends on
`libhwloc`, `libjemalloc`, `libpthread`, and `libgomp`.

## 2. Commands

First, make sure persistent memory is mounted in DAX mode at
`/mnt/pmem`. txMontage and persistent OneFile in this
harness will create heap files prefixed by the user name. Pronto and
Mnemosyne handle heap files by their own and don't have the user name
prefix. Please refer to Section [2.3](#23-different-mount-point) if
NVM is mounted in a different path.

You may run the script from any pwd; it always enters its directory first.

To test scalability:

```bash
./run.sh
```

To plot (Rscript for R language required):

```bash
./data/plot.sh
```

### 2.1. Run Specific Test

**Please read this subsection if you want to manually test specific
data structures on some workloads.**

To build harness for testing all the data structures:

```bash
make
```

Static variables such as `K_SZ` and `V_SZ` can be set while building,
to adjust sizes of *string-typed* keys and values on the workloads. See Section
[3](#3-static-and-dynamic-environment-variables) for more details.

After building, run the following with proper arguments:

```bash
./bin/main -R <rideable_name> -M <test_mode_name> -t <thread_num> -i <duration_of_some_tests> [-v]
``` 

Running `./bin/main` without an argument will print help info. Please
refer to the info for available rideable names and test mode names.

### 2.3. Use Medley and txMontage in Your Code

Medley and txMontage's API can be found in `src/persist/api`.
Currently, all Montage-based data structures in `src/rideables` are
using the *Object-oriented* API by deriving from class `Recoverable`.
Please see data structures `src/rideables/Medley*.hpp` and
`src/rideables/txMontage*.hpp` as examples of detailed usage.

`atomic_lin_var` is the NBTC version of `std::atomic`. As the key of
transaction composition, it is defined in `src/persist/EpochSys.hpp`.
(Sorry for the bad coupling! The project initially wasn't considering
Medley, the system without persistence, would be useful so until we
completed the development of txMontage and started doing experiments.
:P)

### 2.4. Different Mount Point

If NVM is mounted in a different path, please either create a symbolic
link at `/mnt/pmem` (recommended) or search for `/mnt/pmem` in this
repository and replace them appeared in the following files:

```bash
ext/ralloc/src/pm_config.hpp # Ralloc
src/rideables/OneFile/OneFilePTMLF.hpp # Persistent OneFile

run.sh # script for running harness
```

## 3. Static and Dynamic Environment Variables

### 3.1. Static Variables

`K_SZ`: Static variable to control key size (Byte) for maps. By
default it's 32. It needs to be set before compilation to take effect,
e.g., `K_SZ=40 make`. Don't pass values less than 10!

`V_SZ`: Static variable to control value size (Byte) for maps and
queues. By default it's 24. It needs to be set before compilation to
take effect, e.g., `V_SZ=2048 make`.

### 3.2. Dynamic Variables

`prefill`: The number of elements to be prefilled into the tested data
structure. This variable will overwrite the `prefill` argument passed
to Test constructors.

`range`: This decides the range of keys in map tests. This variable
will also overwirte the `range` argument passed to Test constructors.

There are also options mentioned in `./src/persist/README.md` for
configuring Montage parameter, e.g., epoch length, persisting
strategy, and buffering container.

### 3.3. Obsolete Variables

There're some obsolete variables no longer useable and are not
mentioned above. 

To name a few: `KeySize` and `ValueSize`. Inconsistency between them
and `K_SZ` and `V_SZ` will trigger assertion.
Please don't use any not mentioned above, unless you know what exactly
you are doing.
