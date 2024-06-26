# rudis
Redis Re-implemention in Rust for Learning Purpose. In order to make it easy, I will reference the very early version Redis [v1.3.7](https://github.com/redis/redis/releases/tag/v1.3.7).

Since the self-taught purpose of this project, I will ignore some trivial functions (such as virtual memory, cause it's already nonexistent in later redis versions). At the same time, I won't rebuild dynamic strings by myself which is replaced by collections structs in std module of Rust. Most of the commands listed at docs of v1.3.7 are implemented.

Supported types
* string
* list
* set
* zset

_Note: the hash type is ignored, since the underlying collections type are both HashMap._

The Statistics of LOC for v1.3.7:

```txt
===============================================================================
 Language            Files        Lines         Code     Comments       Blanks
===============================================================================
 C                      17        14572        10923         2049         1600
 C Header               15         1259          717          436          106
 CSS                     1           25           20            0            5
 HTML                  107         5040         3957          107          976
 Makefile                1          103           78            4           21
 Ruby                    2          132          101           22            9
 Shell                   1           36           32            1            3
 TCL                     3         2236         1972           47          217
 Plain Text              1            9            0            5            4
===============================================================================
 Total                 148        23412        17800         2671         2941
===============================================================================
```
