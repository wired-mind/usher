package io.cozmic.usher.test;

import java.io.Serializable;

public class Pojo implements Serializable {
	String favoriteColor;
	String name;
	String phone;
	Integer favoriteNumber;
	
	public Pojo(String name, String phone, int favoriteNumber, String favoriteColor) {
		this.favoriteColor = favoriteColor;
		this.name = name;
		this.phone = phone;
		this.favoriteNumber = favoriteNumber;
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

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public int getFavoriteNumber() {
		return favoriteNumber;
	}

	public void setFavoriteNumber(int favoriteNumber) {
		this.favoriteNumber = favoriteNumber;
	}

	public void setFavoriteColor(String favoriteColor) {
		this.favoriteColor = favoriteColor;
	};
	
}
