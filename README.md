JavaZone 2014 Demo Code
=======================

This is the code from my JavaZone 2014 lighting talk [PostgreSQL as MQ](http://2014.javazone.no/presentation.html?id=d63f7405).

Feel free to copy the useful parts of the code. I probably won't release this as a proper library.

About the Code
==============

This code is a pretty raw copy of the code we use in [Fiken](https://fiken.no) for achieving push from incoming mail to
the application that processes the emails. It uses PostgreSQL's LISTEN/NOTIFY feature to do this.

Look at the demos in io.trygvis.jz14.demo on how to use the code.

Resources
=========

* [The slides](https://github.com/trygvis/javazone-2014/blob/master/PostgreSQL%20som%20MQ%20-%20Trygve%20Laugst%C3%B8l.pdf?raw=true).
* [The video (in Norwegian)](https://vimeo.com/105751259)
* The PostgreSQL documentation: [NOTIFY](http://www.postgresql.org/docs/9.3/static/sql-notify.html)/[LISTEN](http://www.postgresql.org/docs/9.3/static/sql-listen.html)
* The alternative PostgreSQL JDBC drivers: https://github.com/impossibl/pgjdbc-ng
