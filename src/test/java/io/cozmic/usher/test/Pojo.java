package io.cozmic.usher.test;

import java.io.Serializable;

public class Pojo implements Serializable {
	String favoriteColor;
	String name;
	String something;
	Integer num;
	
	public Pojo(String name, String something, int num, String favoriteColor) {
		this.favoriteColor = favoriteColor;
		this.name = name;
		this.something = something;
		this.num = num;
	};
	
	public String getFavoriteColor() {
		return favoriteColor;
	}

	public Pojo() {}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSomething() {
		return something;
	}

	public void setSomething(String something) {
		this.something = something;
	}

	public Integer getNum() {
		return num;
	}

	public void setNum(Integer num) {
		this.num = num;
	}

	public void setFavoriteColor(String favoriteColor) {
		this.favoriteColor = favoriteColor;
	};
	
}
