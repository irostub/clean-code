src?=extracode/file_agg_coroutine

setup:
	pip install line_profiler memory_profiler tuna gprof2dot pandas uvloop tqdm aiofile

profile:
	python -m cProfile -o $(src).pstats $(src).py
	gprof2dot -f pstats $(src).pstats | dot -Tpng -o $(src)_output.png

clean:
	find . -type d -name __pycache__ | xargs rm -fr {}
	rm -fr .coverage .pytest_cache/ .hypothesis/ ./extracode/__pytcache__ .__pytcache__/

.PHONY: setup profile clean