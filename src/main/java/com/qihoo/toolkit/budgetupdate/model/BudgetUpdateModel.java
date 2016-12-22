package com.qihoo.toolkit.budgetupdate.model;

public class BudgetUpdateModel {
    private String budgetProductLine = null;
    private String budgetPlan = null;
    
    public String getProductLineBudget() {
        return budgetProductLine;
    }
    
    public void setProductLineBudget(String budget) {
        this.budgetProductLine = budget;
    }
    
    public String getPlanBudget() {
        return budgetPlan;
    }
    
    public void setPlanBudget(String budget) {
        this.budgetPlan = budget;
    }

}
