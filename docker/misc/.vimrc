"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
"
"" These settings are appropriate for editing PolarDB for PostgreSQL code with vim
"
"" You would copy this into your .vimrc or equivalent
"" Copyright (c) 2021, Alibaba Group Holding Limited
"" Licensed under the Apache License, Version 2.0 (the "License");
"" you may not use this file except in compliance with the License.
"" You may obtain a copy of the License at
"
"" http://www.apache.org/licenses/LICENSE-2.0
"
"" Unless required by applicable law or agreed to in writing, software
"" distributed under the License is distributed on an "AS IS" BASIS,
"" WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"" See the License for the specific language governing permissions and
"
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


if match(getcwd(), "/pgsql") >=0 || match(getcwd(), "/postgresql") >=0 || match(getcwd(), "/polardb") >=0
  syntax on
  au BufNewFile,BufRead *.[ch] setlocal noexpandtab autoindent cindent tabstop=4 shiftwidth=4 softtabstop=0 cinoptions="(0,t0"
"  this will highlight the 80th column, uncomment it if need it
"  set colorcolumn=80
"  highlight ColorColumn ctermbg=52
endif

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
