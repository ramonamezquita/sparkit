build:
	mkdir -p ./dist
	cp ./src/main.py ./dist
	cd ./src && zip -x main.py -r ../dist/sparkit.zip .