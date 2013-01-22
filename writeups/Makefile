all: class_graph.png a1.pdf a2.pdf a3.pdf a4.pdf

%.pdf: %.tex
	pdflatex -halt-on-error $< | tail -n 2

class_graph.png: class_graph.dot
	dot -Tpng class_graph.dot >class_graph.png

clean:
	rm *.log *.aux *.pdf
